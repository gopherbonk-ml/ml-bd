import argparse, os, glob
import cv2
import numpy as np
from pathlib import Path

CLASSES = {"whole": 0, "broken": 1}


def sample_frame_indices(total_frames, fps, every_s):
    step = max(1, int(round(fps * every_s)))
    return list(range(0, total_frames, step))


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--videos", required=True, help="folder with .mp4 videos")
    ap.add_argument("--out-img", required=True)
    ap.add_argument("--out-lbl", required=True)
    ap.add_argument("--every", type=float, default=0.5, help="seconds between frames")
    ap.add_argument("--bbox-px", type=int, default=32, help="bbox side in pixels around click")
    ap.add_argument("--class", dest="default_class", choices=list(CLASSES), default="whole")
    args = ap.parse_args()

    out_img = Path(args.out_img); out_lbl = Path(args.out_lbl)
    ensure_dir(out_img); ensure_dir(out_lbl)

    videos = sorted(glob.glob(os.path.join(args.videos, "**/*.MP4"), recursive=True))
    if not videos:
        print("No videos found.")
        return

    for vpath in videos:
        cap = cv2.VideoCapture(vpath)
        if not cap.isOpened():
            print(f"Cannot open {vpath}")
            continue
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        idxs = sample_frame_indices(total, fps, args.every)

        print(f"Annotating {vpath} | fps={fps:.2f} frames={total} samples={len(idxs)}")
        cur = 0
        point = None
        cur_class = CLASSES[args.default_class]

        while True:
            if cur < 0: cur = 0
            if cur >= len(idxs): break
            fidx = idxs[cur]
            cap.set(cv2.CAP_PROP_POS_FRAMES, fidx)
            ok, frame = cap.read()
            if not ok: break
            h, w = frame.shape[:2]

            disp = frame.copy()
            info = f"{os.path.basename(vpath)}  t={fidx/fps:.2f}s  sample {cur+1}/{len(idxs)}  class={'whole' if cur_class==0 else 'broken'}"
            cv2.putText(disp, info, (10, 24), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0,255,255), 2, cv2.LINE_AA)

            if point is not None:
                cx, cy = point
                s = args.bbox_px
                x1, y1 = max(0, cx - s//2), max(0, cy - s//2)
                x2, y2 = min(w-1, cx + s//2), min(h-1, cy + s//2)
                cv2.rectangle(disp, (x1,y1), (x2,y2), (0,255,0), 2)

            cv2.imshow("annotator", disp)

            def on_mouse(event, x, y, flags, param):
                nonlocal point
                if event == cv2.EVENT_LBUTTONDOWN:
                    point = (x, y)
            cv2.setMouseCallback("annotator", on_mouse)

            key = cv2.waitKey(30) & 0xFF
            if key == ord('q'):
                break
            elif key == ord('n'):
                point = None; cur += 1
            elif key == ord('p'):
                point = None; cur -= 1
            elif key == ord('r'):
                point = None
            elif key == ord('1'):
                cur_class = 0
            elif key == ord('2'):
                cur_class = 1
            elif key == ord('s'):
                # save image + label (only if point set)
                if point is None:
                    print("No point; press left mouse button to mark center.")
                    continue
                cx, cy = point; s = args.bbox_px
                x1, y1 = max(0, cx - s//2), max(0, cy - s//2)
                x2, y2 = min(w-1, cx + s//2), min(h-1, cy + s//2)
                # normalize for YOLO: (cx,cy,w,h) in 0..1
                bb_w = (x2 - x1) / w
                bb_h = (y2 - y1) / h
                cx_n = (x1 + x2) / 2.0 / w
                cy_n = (y1 + y2) / 2.0 / h

                # write image
                stem = f"{Path(vpath).stem}_f{fidx:06d}"
                img_path = out_img / f"{stem}.jpg"
                lbl_path = out_lbl / f"{stem}.txt"
                cv2.imwrite(str(img_path), frame)

                with open(lbl_path, 'w') as f:
                    f.write(f"{cur_class} {cx_n:.6f} {cy_n:.6f} {bb_w:.6f} {bb_h:.6f}\n")
                print(f"Saved {img_path.name}, {lbl_path.name}")
                point = None; cur += 1

        cap.release()
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()