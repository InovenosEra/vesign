"""Generate a transparent 'S&P 500' text PNG.

Used for both SPY and VOO. Transparent background + white text so the
visual reads as plain text floating in the dark UI cell, not as a tile.

Renders at 2x then LANCZOS-downsamples for crisp typography.
"""
from PIL import Image, ImageDraw, ImageFont

OUT_SIZE = 256
RENDER = 512  # render at 2x then downsample

WHITE = (232, 234, 240, 255)  # matches App.css var(--text)

img = Image.new("RGBA", (RENDER, RENDER), (0, 0, 0, 0))  # transparent
draw = ImageDraw.Draw(img)

# Helvetica Neue Bold via .ttc index 1
font_path = "/System/Library/Fonts/HelveticaNeue.ttc"
font = ImageFont.truetype(font_path, 130, index=1)

text = "S&P 500"
bbox = draw.textbbox((0, 0), text, font=font)
w = bbox[2] - bbox[0]
h = bbox[3] - bbox[1]
x = (RENDER - w) // 2 - bbox[0]
y = (RENDER - h) // 2 - bbox[1]
draw.text((x, y), text, font=font, fill=WHITE)

img = img.resize((OUT_SIZE, OUT_SIZE), Image.LANCZOS)
img.save("/tmp/sp500_logo.png", "PNG", optimize=True)
print("wrote /tmp/sp500_logo.png")
