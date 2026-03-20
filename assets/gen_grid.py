"""Generate a 4x4 dog breed grid image for social media."""

from PIL import Image, ImageDraw, ImageFont
import os

DOGS = [
    ("Philosopher", "INTP", "#E0EFDA", "dog-philosopher.png"),
    ("Architect",   "INTJ", "#D0D8C4", "dog-architect.png"),
    ("Intern",      "ENFP", "#FFE0EC", "dog-intern.png"),
    ("Commander",   "ENTJ", "#E8D0D8", "dog-commander.png"),
    ("Rereader",    "ISTJ", "#FFE8D0", "dog-rereader.png"),
    ("Caretaker",   "ISFJ", "#F5E6D8", "dog-caretaker.png"),
    ("Perfectionist","INFJ","#E8D8F0", "dog-perfectionist.png"),
    ("Mentor",      "ENFJ", "#D8D0E8", "dog-mentor.png"),
    ("Vampire",     "ISTP", "#D0D4DC", "dog-vampire.png"),
    ("Drifter",     "ISFP", "#F0E8F8", "dog-drifter.png"),
    ("Goldfish",    "ESFP", "#D8F0F4", "dog-goldfish.png"),
    ("Helper",      "ESFJ", "#E0F0D0", "dog-helper.png"),
    ("Brute",       "ESTJ", "#F4D0C8", "dog-brute.png"),
    ("Ghost",       "INFP", "#E8E8E8", "dog-ghost.png"),
    ("Speedrunner", "ESTP", "#FFF0C8", "dog-speedrunner.png"),
    ("Googler",     "ENTP", "#D0E0F4", "dog-googler.png"),
]

def hex_to_rgb(h):
    h = h.lstrip('#')
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

def main():
    # Layout
    cols, rows = 4, 4
    cell_w, cell_h = 270, 320
    dog_size = 180
    padding = 16
    header_h = 80
    footer_h = 60
    canvas_w = cols * cell_w + (cols + 1) * padding
    canvas_h = header_h + rows * cell_h + (rows + 1) * padding + footer_h
    bg_color = (15, 15, 15)

    img = Image.new("RGB", (canvas_w, canvas_h), bg_color)
    draw = ImageDraw.Draw(img)

    # Fonts - try system fonts
    try:
        font_title = ImageFont.truetype("seguisb.ttf", 28)
        font_name = ImageFont.truetype("seguisb.ttf", 16)
        font_mbti = ImageFont.truetype("segoeui.ttf", 12)
        font_header = ImageFont.truetype("seguisb.ttf", 32)
        font_footer = ImageFont.truetype("segoeui.ttf", 14)
    except:
        font_title = ImageFont.load_default()
        font_name = font_title
        font_mbti = font_title
        font_header = font_title
        font_footer = font_title

    # Header
    header_text = "What kind of dog is your AI?"
    bbox = draw.textbbox((0, 0), header_text, font=font_header)
    tw = bbox[2] - bbox[0]
    draw.text(((canvas_w - tw) / 2, 24), header_text, fill=(240, 240, 240), font=font_header)

    # Grid
    dogs_dir = os.path.join(os.path.dirname(__file__), "dogs")

    for idx, (name, mbti, color, filename) in enumerate(DOGS):
        row = idx // cols
        col = idx % cols
        x = padding + col * (cell_w + padding)
        y = header_h + padding + row * (cell_h + padding)

        # Card background
        card_color = hex_to_rgb(color)
        draw.rounded_rectangle([x, y, x + cell_w, y + cell_h], radius=16, fill=card_color)

        # Dog image
        dog_path = os.path.join(dogs_dir, filename)
        if os.path.exists(dog_path):
            dog = Image.open(dog_path).convert("RGBA")
            dog = dog.resize((dog_size, dog_size), Image.LANCZOS)
            # Center dog in card
            dx = x + (cell_w - dog_size) // 2
            dy = y + 16
            # Paste with alpha
            img.paste(dog, (dx, dy), dog)

        # Name
        bbox = draw.textbbox((0, 0), name, font=font_name)
        tw = bbox[2] - bbox[0]
        draw.text((x + (cell_w - tw) / 2, y + dog_size + 24), name, fill=(30, 30, 30), font=font_name)

        # MBTI
        bbox = draw.textbbox((0, 0), mbti, font=font_mbti)
        tw = bbox[2] - bbox[0]
        draw.text((x + (cell_w - tw) / 2, y + dog_size + 46), mbti, fill=(100, 100, 100), font=font_mbti)

    # Footer
    footer_text = "punkgo.ai/roast"
    bbox = draw.textbbox((0, 0), footer_text, font=font_footer)
    tw = bbox[2] - bbox[0]
    draw.text(((canvas_w - tw) / 2, canvas_h - 36), footer_text, fill=(57, 255, 20), font=font_footer)

    # Brand label
    label = "PUNKGO ROAST"
    bbox = draw.textbbox((0, 0), label, font=font_mbti)
    tw = bbox[2] - bbox[0]
    draw.text(((canvas_w - tw) / 2, canvas_h - 54), label, fill=(80, 80, 80), font=font_mbti)

    # Save
    out_path = os.path.join(os.path.dirname(__file__), "roast-grid-16.png")
    img.save(out_path, "PNG", quality=95)
    print(f"Saved to {out_path} ({img.size[0]}x{img.size[1]})")

if __name__ == "__main__":
    main()
