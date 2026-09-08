#!/usr/bin/env python3
"""Generate Kafka Eye icons.

The mark is the Kafka logo silhouette with its circular "nodes" replaced by
eyes. Drawn at 4x and downsampled so the curves antialias instead of showing
the staircase edges the original hand-plotted icons had.

Colour roles are inverted relative to v1.x: the orange is now the background
and the Kafka mark is dark, rather than an orange outline floating on black.
"""

from PIL import Image, ImageDraw

BG = (216, 106, 42)        # Kafka orange — now the background
MARK = (26, 26, 26)        # dark logo silhouette
SCLERA = (255, 255, 255)
IRIS = (58, 130, 194)      # blue iris, readable against both bg and mark
PUPIL = (16, 16, 16)

S = 512          # supersampled working size
SCALE = S / 128  # design coordinates are in 128px space


def px(v):
    return int(round(v * SCALE))


def draw_eye(d, cx, cy, r):
    """Eye as concentric circles: white sclera, blue iris, dark pupil."""
    cx, cy, r = px(cx), px(cy), px(r)
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=SCLERA)
    ir = int(r * 0.62)
    d.ellipse([cx - ir, cy - ir, cx + ir, cy + ir], fill=IRIS)
    pr = int(r * 0.30)
    d.ellipse([cx - pr, cy - pr, cx + pr, cy + pr], fill=PUPIL)


def build(size):
    img = Image.new('RGBA', (S, S), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)

    d.rounded_rectangle([0, 0, S - 1, S - 1], radius=px(22), fill=BG)

    # Kafka mark: vertical spine with two branch arms; the round nodes on the
    # right are the ones replaced by eyes.
    spine_w = 9
    spine_x = 46
    eye_x = 80
    d.rounded_rectangle(
        [px(spine_x - spine_w / 2), px(32), px(spine_x + spine_w / 2), px(96)],
        radius=px(spine_w / 2), fill=MARK)

    d.line([px(spine_x), px(52), px(eye_x), px(46)], fill=MARK, width=px(7))
    d.line([px(spine_x), px(76), px(eye_x), px(82)], fill=MARK, width=px(7))

    # Spine end caps
    for cy in (32, 96):
        r = px(9)
        cx, cyp = px(spine_x), px(cy)
        d.ellipse([cx - r, cyp - r, cx + r, cyp + r], fill=MARK)

    draw_eye(d, eye_x, 46, 14)
    draw_eye(d, eye_x, 82, 14)

    return img.resize((size, size), Image.LANCZOS)


if __name__ == '__main__':
    for size in (16, 48, 128):
        build(size).save(f'icon{size}.png')
        print(f'wrote icon{size}.png')
