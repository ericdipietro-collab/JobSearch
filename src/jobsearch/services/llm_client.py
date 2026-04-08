"""LLM client abstraction supporting multiple providers (Gemini, OpenAI, Ollama)."""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Optional

from google import genai

logger = logging.getLogger(__name__)

_RETRY_DELAYS = [7, 15, 30]  # seconds between retries on 429


def _parse_retry_after(error_str: str) -> Optional[float]:
    """Extract retry_delay seconds from a Gemini 429 error string if present."""
    match = re.search(r'retry_delay\s*\{\s*seconds:\s*(\d+)', error_str)
    if match:
        return float(match.group(1)) + 1
    return None


class LLMClient:
    """Abstract LLM interface supporting Gemini, OpenAI, and Ollama with fallback logic."""

    def __init__(
        self,
        google_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        preferred_provider: Optional[str] = None,
        ollama_base_url: Optional[str] = None,
        ollama_model: Optional[str] = None,
    ):
        self.google_api_key = google_api_key or os.getenv("GOOGLE_API_KEY", "").strip()
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY", "").strip()
        self.preferred_provider = preferred_provider
        self.ollama_base_url = (ollama_base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")).rstrip("/")
        self.ollama_model = ollama_model or os.getenv("OLLAMA_MODEL", "llama3.2")
        self._active_provider = None

        self._gemini_client = None
        if self.google_api_key:
            self._gemini_client = genai.Client(api_key=self.google_api_key)

    def _detect_provider(self) -> Optional[str]:
        """Detect which provider to use based on availability and preference."""
        if self.preferred_provider:
            if self.preferred_provider == "gemini" and self.google_api_key:
                return "gemini"
            elif self.preferred_provider == "openai" and self.openai_api_key:
                return "openai"
            elif self.preferred_provider == "ollama":
                return "ollama"

        if self.google_api_key:
            return "gemini"
        elif self.openai_api_key:
            return "openai"

        return None

    def generate(self, prompt: str) -> tuple[str, Optional[dict]]:
        """
        Generate text from LLM with automatic retry + backoff on rate limits.

        Returns:
            Tuple of (generated_text, usage_metadata_dict)
            usage_metadata_dict contains: {"total_tokens": int, "provider": str}
        """
        provider = self._detect_provider()

        if not provider:
            raise ValueError(
                "No LLM provider configured. Set GOOGLE_API_KEY, OPENAI_API_KEY, or configure Ollama."
            )

        if provider == "gemini":
            return self._generate_with_retry(prompt)
        elif provider == "openai":
            return self._generate_openai(prompt)
        elif provider == "ollama":
            return self._generate_ollama(prompt)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    def _is_rate_limit_error(self, e: Exception) -> bool:
        s = str(e)
        return "429" in s or "quota" in s.lower() or "rate" in s.lower() or "resource exhausted" in s.lower()

    def _is_daily_quota_error(self, e: Exception) -> bool:
        s = str(e).lower()
        return "per day" in s or "daily" in s or "rpd" in s or "limit: 0" in s

    def _generate_with_retry(self, prompt: str) -> tuple[str, Optional[dict]]:
        """Gemini call with exponential backoff on RPM/TPM errors, OpenAI fallback on daily quota."""
        last_exc = None
        for attempt, delay in enumerate(_RETRY_DELAYS + [None]):
            try:
                return self._generate_gemini(prompt)
            except Exception as e:
                last_exc = e
                if not self._is_rate_limit_error(e):
                    raise

                if self._is_daily_quota_error(e):
                    # Daily cap hit — retrying won't help, fall back immediately
                    if self.openai_api_key:
                        logger.warning("Gemini daily quota exceeded, falling back to OpenAI.")
                        return self._generate_openai(prompt)
                    raise

                if delay is None:
                    break

                # Honour Gemini's suggested retry_delay if provided, else use our schedule
                wait = _parse_retry_after(str(e)) or delay
                logger.warning(f"Gemini rate limited (attempt {attempt + 1}), retrying in {wait}s...")
                time.sleep(wait)

        # All retries exhausted — try OpenAI before giving up
        if self.openai_api_key:
            logger.warning("Gemini rate limit retries exhausted, falling back to OpenAI.")
            return self._generate_openai(prompt)

        raise last_exc

    def _generate_gemini(self, prompt: str) -> tuple[str, Optional[dict]]:
        """Generate using Google Gemini."""
        try:
            response = self._gemini_client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=prompt,
            )

            usage_metadata = None
            if hasattr(response, "usage_metadata") and response.usage_metadata:
                usage_metadata = {
                    "total_tokens": response.usage_metadata.total_token_count,
                    "provider": "gemini",
                }

            return response.text, usage_metadata
        except Exception as e:
            logger.error(f"Gemini generation failed: {e}")
            raise

    def _generate_openai(self, prompt: str) -> tuple[str, Optional[dict]]:
        """Generate using OpenAI GPT-4o-mini."""
        try:
            from openai import OpenAI

            client = OpenAI(api_key=self.openai_api_key)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
            )

            usage_metadata = {
                "total_tokens": response.usage.total_tokens,
                "provider": "openai",
            }

            return response.choices[0].message.content, usage_metadata
        except Exception as e:
            logger.error(f"OpenAI generation failed: {e}")
            raise

    def _generate_ollama(self, prompt: str) -> tuple[str, Optional[dict]]:
        """Generate using a local Ollama instance via its OpenAI-compatible API."""
        try:
            from openai import OpenAI

            client = OpenAI(
                base_url=f"{self.ollama_base_url}/v1",
                api_key="ollama",  # Ollama doesn't require a real key
            )
            # Local models need a strong system prompt for reliable JSON output.
            # Temperature 0.0 suppresses hallucinated preamble/postamble.
            response = client.chat.completions.create(
                model=self.ollama_model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a JSON extraction assistant. "
                            "Respond with valid JSON only. "
                            "Do not include any explanation, preamble, commentary, or markdown formatting. "
                            "Do not wrap output in code fences or backticks. "
                            "Output raw JSON starting with { or [ and nothing else."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
            )

            usage_metadata = {
                "total_tokens": response.usage.total_tokens if response.usage else 0,
                "provider": "ollama",
                "model": self.ollama_model,
            }

            return response.choices[0].message.content, usage_metadata
        except Exception as e:
            logger.error(f"Ollama generation failed: {e}")
            raise

    def is_local(self) -> bool:
        """Return True when running against a local provider (Ollama)."""
        return self._detect_provider() == "ollama"

    @staticmethod
    def strip_json_response(text: str) -> str:
        """
        Strip all markdown code fences and surrounding whitespace from an LLM response.
        Handles ```json, ```, and plain backtick variants.
        """
        text = text.strip()
        # Remove opening fence variants
        for fence in ("```json\n", "```json", "```\n", "```"):
            if text.startswith(fence):
                text = text[len(fence):]
                break
        # Remove closing fence
        if text.endswith("```"):
            text = text[:-3]
        return text.strip()

    def get_active_provider(self) -> str:
        """Return the name of the currently active provider."""
        provider = self._detect_provider()
        return provider or "none"
