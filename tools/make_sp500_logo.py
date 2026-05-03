"""Generate a nicer 'S&P 500' logo PNG.

Used for both SPY and VOO (both track the S&P 500 — they get the same
hand-crafted tile, distinguished only by the ticker symbol shown elsewhere).

Renders at 2x then downsamples with LANCZOS for crisp typography on retina.
"""
import math
from PIL import Image, ImageDraw, ImageFont

OUT_SIZE = 256
RENDER = 512                 # render at 2x then downsample for crisp edges

# Palette — deeper navy with subtle gradient + warmer gold
NAVY_TOP = (24, 56, 102)     # lighter navy near top
NAVY_BOTTOM = (6, 18, 42)    # nearly black at bottom
GOLD = (218, 178, 60)
GOLD_BRIGHT = (244, 208, 96)
WHITE = (248, 248, 248)


def vertical_gradient(size, top_color, bottom_color):
    img = Image.new("RGB", (size, size), top_color)
    draw = ImageDraw.Draw(img)
    for y in range(size):
        t = y / (size - 1)
        r = int(top_color[0] * (1 - t) + bottom_color[0] * t)
        g = int(top_color[1] * (1 - t) + bottom_color[1] * t)
        b = int(top_color[2] * (1 - t) + bottom_color[2] * t)
        draw.line([(0, y), (size, y)], fill=(r, g, b))
    return img


img = vertical_gradient(RENDER, NAVY_TOP, NAVY_BOTTOM)
draw = ImageDraw.Draw(img)

# Subtle inner highlight ring (very faint, gives depth)
inner_offset = 26
draw.rounded_rectangle(
    [(inner_offset, inner_offset), (RENDER - inner_offset, RENDER - inner_offset)],
    radius=44,
    outline=(40, 80, 130),
    width=2,
)

# Main gold border
border_offset = 16
draw.rounded_rectangle(
    [(border_offset, border_offset), (RENDER - border_offset, RENDER - border_offset)],
    radius=54,
    outline=GOLD,
    width=4,
)

# Fonts — Helvetica Neue Bold via .ttc index 1
font_path = "/System/Library/Fonts/HelveticaNeue.ttc"
font_sp = ImageFont.truetype(font_path, 130, index=1)
font_500 = ImageFont.truetype(font_path, 220, index=1)


def centered(text, font, y, fill):
    bbox = draw.textbbox((0, 0), text, font=font)
    w = bbox[2] - bbox[0]
    x = (RENDER - w) // 2 - bbox[0]
    draw.text((x, y), text, font=font, fill=fill)
    return bbox[3] - bbox[1]


# "S&P" — white, top
centered("S&P", font_sp, 80, WHITE)

# Gold divider — short, centered
div_y = 230
div_len = 110
div_x = (RENDER - div_len) // 2
draw.rectangle([(div_x, div_y), (div_x + div_len, div_y + 4)], fill=GOLD_BRIGHT)

# "500" — gold, larger, below divider
centered("500", font_500, 254, GOLD_BRIGHT)

# Downsample for crispness
img = img.resize((OUT_SIZE, OUT_SIZE), Image.LANCZOS)
img.save("/tmp/sp500_logo.png", "PNG", optimize=True)
print("wrote /tmp/sp500_logo.png")
