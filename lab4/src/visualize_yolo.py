
import argparse, os, glob
from pathlib import Path
import cv2
from ultralytics import YOLO

COLORS = [(0,255,0),(0,128,255)]  # whole, broken
NAMES = {0:"whole",1:"broken"}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--videos", required=True)
    ap.add_argument("--weights", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--conf", type=float, default=0.25)
    args = ap.parse_args()

    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)
    model = YOLO(args.weights)

    vids = [sorted(glob.glob(os.path.join(args.videos, "**/*.MP4"), recursive=True))[0]]
    for v in vids:
        cap = cv2.VideoCapture(v)
        if not cap.isOpened():
            print("Cannot open", v); continue
        fps = cap.get(cv2.CAP_PROP_FPS) or 30
        w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        outp = outdir / (Path(v).stem + "_viz.mp4")
        writer = cv2.VideoWriter(str(outp), fourcc, fps, (w,h))

        while True:
            ok, frame = cap.read()
            if not ok: break

            res = model.predict(frame, conf=args.conf, verbose=False)[0]
            for b in res.boxes:
                cls = int(b.cls.item())
                if cls not in NAMES: continue
                x1,y1,x2,y2 = map(int, b.xyxy[0].tolist())
                conf = float(b.conf.item())
                cv2.rectangle(frame, (x1,y1), (x2,y2), COLORS[cls], 2)
                cv2.putText(frame, f"{NAMES[cls]} {conf:.2f}", (x1,y1-6), cv2.FONT_HERSHEY_SIMPLEX, 0.6, COLORS[cls], 2)

            writer.write(frame)
        cap.release(); writer.release()
        print("Wrote", outp)

if __name__ == "__main__":
    main()