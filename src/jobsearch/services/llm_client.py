"""LLM client abstraction supporting multiple providers (Gemini, OpenAI)."""

from __future__ import annotations

import logging
import os
from typing import Optional

import google.generativeai as genai

logger = logging.getLogger(__name__)


class LLMClient:
    """Abstract LLM interface supporting Gemini and OpenAI with fallback logic."""

    def __init__(
        self,
        google_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        preferred_provider: Optional[str] = None,
    ):
        """
        Initialize LLM client with available providers.

        Args:
            google_api_key: Google API key for Gemini (defaults to GOOGLE_API_KEY env)
            openai_api_key: OpenAI API key (defaults to OPENAI_API_KEY env)
            preferred_provider: Preferred provider ('gemini' or 'openai'). Auto-detects if None.
        """
        self.google_api_key = google_api_key or os.getenv("GOOGLE_API_KEY", "").strip()
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY", "").strip()
        self.preferred_provider = preferred_provider
        self._active_provider = None

        # Configure available providers
        if self.google_api_key:
            genai.configure(api_key=self.google_api_key)

    def _detect_provider(self) -> Optional[str]:
        """Detect which provider to use based on availability and preference."""
        if self.preferred_provider:
            if self.preferred_provider == "gemini" and self.google_api_key:
                return "gemini"
            elif self.preferred_provider == "openai" and self.openai_api_key:
                return "openai"

        # Default: Gemini first, then OpenAI
        if self.google_api_key:
            return "gemini"
        elif self.openai_api_key:
            return "openai"

        return None

    def generate(self, prompt: str) -> tuple[str, Optional[dict]]:
        """
        Generate text from LLM.

        Args:
            prompt: The prompt to send to the LLM

        Returns:
            Tuple of (generated_text, usage_metadata_dict)
            usage_metadata_dict contains: {"total_tokens": int, "provider": str}
        """
        provider = self._detect_provider()

        if not provider:
            raise ValueError(
                "No LLM provider configured. Set GOOGLE_API_KEY or OPENAI_API_KEY."
            )

        if provider == "gemini":
            return self._generate_gemini(prompt)
        elif provider == "openai":
            return self._generate_openai(prompt)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    def _generate_gemini(self, prompt: str) -> tuple[str, Optional[dict]]:
        """Generate using Google Gemini."""
        try:
            model = genai.GenerativeModel("gemini-1.5-flash")
            response = model.generate_content(prompt)

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

    def get_active_provider(self) -> str:
        """Return the name of the currently active provider."""
        provider = self._detect_provider()
        return provider or "none"
