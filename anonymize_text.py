import os
import random
from PIL import Image, ImageDraw, ImageFont
from kraken import binarization, pageseg

FAKE_COMPANIES = [
    "Acme Corp", "Globex", "Initech", "Umbrella Corp", "Stark Industries",
    "Wayne Enterprises", "Massive Dynamic", "Hooli", "Cyberdyne", "Soylent",
    "Pied Piper", "Goliath National", "Dunder Mifflin", "Aperture Science"
]

FAKE_ROLES = [
    "Software Engineer", "Product Manager", "Data Scientist", "UX Designer",
    "DevOps Engineer", "QA Automation", "System Admin", "Security Analyst",
    "Frontend Developer", "Backend Engineer", "Engineering Manager"
]

def get_bg_color(im, box):
    # Sample a few pixels just outside the left edge of the box
    x0, y0, x1, y1 = box
    sample_x = max(0, x0 - 5)
    sample_y = (y0 + y1) // 2
    try:
        return im.getpixel((sample_x, sample_y))
    except Exception:
        return (255, 255, 255) # Fallback to white

def anonymize_with_text():
    screenshot_dir = "docs/Screenshots"
    
    # Try to load a standard font
    try:
        font = ImageFont.truetype("segoeui.ttf", 14)
    except IOError:
        try:
            font = ImageFont.truetype("arial.ttf", 14)
        except IOError:
            font = ImageFont.load_default()

    config = {
        "jobmatches.png": {
            "columns": [
                {"x_range": (240, 420), "type": "company"}, # Company
                {"x_range": (420, 700), "type": "role"}    # Role
            ],
            "y_min": 350
        },
        "myapplications.png": {
            "columns": [
                {"x_range": (250, 450), "type": "company"}, # Upcoming interview company
                {"x_range": (350, 520), "type": "company"}, # Table Company
                {"x_range": (520, 850), "type": "role"}     # Table Role
            ],
            "y_min": 180
        },
        "weeklyactivity.png": {
            "columns": [
                {"x_range": (300, 450), "type": "company"}, # Table Company
                {"x_range": (450, 800), "type": "role"}     # Table Role
            ],
            "y_min": 600,
            "hide_boxes": [
                (250, 840, 1100, 1229) # Just white out the bottom report to be safe
            ]
        }
    }

    for name, cfg in config.items():
        p = os.path.join(screenshot_dir, name)
        if not os.path.exists(p):
            continue
            
        print(f"Processing {name} with text replacement...")
        im = Image.open(p).convert("RGB")
        draw = ImageDraw.Draw(im)
        
        bw = binarization.nlbin(im)
        seg = pageseg.segment(bw)
        
        # Process special hide boxes (fill with white)
        for box in cfg.get("hide_boxes", []):
            draw.rectangle(box, fill=(245, 246, 250)) # Light gray typical for backgrounds
            
        y_min = cfg.get("y_min", 0)
        columns = cfg.get("columns", [])
        
        for line in seg.lines:
            x0, y0, x1, y1 = line.bbox
            
            if y0 < y_min:
                continue
                
            # Determine which column this text is in
            for col in columns:
                xr_start, xr_end = col["x_range"]
                # If center of the text box is in the column range
                center_x = (x0 + x1) // 2
                if xr_start <= center_x <= xr_end:
                    bg_color = get_bg_color(im, line.bbox)
                    
                    # Fill the box to erase original text
                    draw.rectangle([x0-2, y0-2, x1+2, y1+2], fill=bg_color)
                    
                    # Choose text
                    if col["type"] == "company":
                        text = random.choice(FAKE_COMPANIES)
                    else:
                        text = random.choice(FAKE_ROLES)
                        
                    # Draw new text
                    # We use a dark grey color common in UIs
                    draw.text((x0, y0), text, fill=(50, 50, 50), font=font)
                    break # Only process once per text box
                    
        im.save(p)
        print(f"Saved {name}.")

if __name__ == "__main__":
    anonymize_with_text()
