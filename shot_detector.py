# Avi Shah - Basketball Shot Detector/Tracker - July 2023

from ultralytics import YOLO
import cv2
import cvzone
import math
import numpy as np
from utils import score, detect_down, detect_up, in_hoop_region, clean_hoop_pos, clean_ball_pos, get_device


class ShotDetector:
    def __init__(self):
        # Load the YOLO model created from main.py - change text to your relative path
        self.overlay_text = "Waiting..."
        self.model = YOLO("best.pt")
        
        # Uncomment this line to accelerate inference. Note that this may cause errors in some setups.
        #self.model.half()
        
        self.class_names = ['Basketball', 'Basketball Hoop']
        self.device = get_device()
        # Uncomment line below to use webcam (I streamed to my iPhone using Iriun Webcam)
        # self.cap = cv2.VideoCapture(0)

        # Use video - replace text with your video path
        self.cap = cv2.VideoCapture("video_levana.mp4")

        self.ball_pos = []  # array of tuples ((x_pos, y_pos), frame count, width, height, conf)
        self.hoop_pos = []  # array of tuples ((x_pos, y_pos), frame count, width, height, conf)

        self.frame_count = 0
        self.frame = None

        self.makes = 0
        self.attempts = 0

        # Used to detect shots (upper and lower region)
        self.up = False
        self.down = False
        self.up_frame = 0
        self.down_frame = 0

        # Used for green and red colors after make/miss
        self.fade_frames = 20
        self.fade_counter = 0
        self.overlay_color = (0, 0, 0)

        self.run()

    def resize_frame_to_fit_screen(self, frame):
        """
        Resize the frame to fit the screen while maintaining aspect ratio.
        Uses 80% of screen width and height as maximum dimensions.
        """
        # Get screen dimensions (approximate for Windows)
        screen_width = 1920  # Default screen width
        screen_height = 1080  # Default screen height
        
        # Use 80% of screen size to ensure it fits
        max_width = int(screen_width * 0.8)
        max_height = int(screen_height * 0.8)
        
        # Get original frame dimensions
        height, width = frame.shape[:2]
        
        # Calculate scaling factor to fit within max dimensions while maintaining aspect ratio
        scale_x = max_width / width
        scale_y = max_height / height
        scale = min(scale_x, scale_y)
        
        # Only resize if the frame is larger than the maximum allowed size
        if scale < 1:
            new_width = int(width * scale)
            new_height = int(height * scale)
            resized_frame = cv2.resize(frame, (new_width, new_height), interpolation=cv2.INTER_AREA)
            return resized_frame
        else:
            return frame

    def run(self):
        while True:
            ret, self.frame = self.cap.read()

            if not ret:
                # End of the video or an error occurred
                break

            results = self.model(self.frame, stream=True, device=self.device)

            for r in results:
                boxes = r.boxes
                for box in boxes:
                    # Bounding box
                    x1, y1, x2, y2 = box.xyxy[0]
                    x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
                    w, h = x2 - x1, y2 - y1

                    # Confidence
                    conf = math.ceil((box.conf[0] * 100)) / 100

                    # Class Name
                    cls = int(box.cls[0])
                    current_class = self.class_names[cls]

                    center = (int(x1 + w / 2), int(y1 + h / 2))

                    # Only create ball points if high confidence or near hoop
                    if (conf > .3 or (in_hoop_region(center, self.hoop_pos) and conf > 0.15)) and current_class == "Basketball":
                        self.ball_pos.append((center, self.frame_count, w, h, conf))
                        cvzone.cornerRect(self.frame, (x1, y1, w, h))

                    # Create hoop points if high confidence
                    if conf > .5 and current_class == "Basketball Hoop":
                        self.hoop_pos.append((center, self.frame_count, w, h, conf))
                        cvzone.cornerRect(self.frame, (x1, y1, w, h))

            self.clean_motion()
            self.shot_detection()
            self.display_score()
            self.frame_count += 1

            # Resize frame to fit screen
            display_frame = self.resize_frame_to_fit_screen(self.frame)
            cv2.imshow('Frame', display_frame)

            # Close if 'q' is clicked
            if cv2.waitKey(1) & 0xFF == ord('q'):  # higher waitKey slows video down, use 1 for webcam
                break

        self.cap.release()
        cv2.destroyAllWindows()

    def clean_motion(self):
        # Clean and display ball motion
        self.ball_pos = clean_ball_pos(self.ball_pos, self.frame_count)
        for i in range(0, len(self.ball_pos)):
            cv2.circle(self.frame, self.ball_pos[i][0], 2, (0, 0, 255), 2)

        # Clean hoop motion and display current hoop center
        if len(self.hoop_pos) > 1:
            self.hoop_pos = clean_hoop_pos(self.hoop_pos)
            cv2.circle(self.frame, self.hoop_pos[-1][0], 2, (128, 128, 0), 2)

    def shot_detection(self):
        if len(self.hoop_pos) > 0 and len(self.ball_pos) > 0:
            # Detecting when ball is in 'up' and 'down' area - ball can only be in 'down' area after it is in 'up'
            if not self.up:
                self.up = detect_up(self.ball_pos, self.hoop_pos)
                if self.up:
                    self.up_frame = self.ball_pos[-1][1]

            if self.up and not self.down:
                self.down = detect_down(self.ball_pos, self.hoop_pos)
                if self.down:
                    self.down_frame = self.ball_pos[-1][1]

            # If ball goes from 'up' area to 'down' area in that order, increase attempt and reset
            if self.frame_count % 10 == 0:
                if self.up and self.down and self.up_frame < self.down_frame:
                    self.attempts += 1
                    self.up = False
                    self.down = False

                    # If it is a make, put a green overlay and display "完美"
                    if score(self.ball_pos, self.hoop_pos):
                        self.makes += 1
                        self.overlay_color = (0, 255, 0)  # Green for make
                        self.overlay_text = "Make"
                        self.fade_counter = self.fade_frames

                    else:
                        self.overlay_color = (255, 0, 0)  # Red for miss
                        self.overlay_text = "Miss"
                        self.fade_counter = self.fade_frames

    def display_score(self):
        # Add text
        text = str(self.makes) + " / " + str(self.attempts)
        cv2.putText(self.frame, text, (50, 125), cv2.FONT_HERSHEY_SIMPLEX, 3, (255, 255, 255), 6)
        cv2.putText(self.frame, text, (50, 125), cv2.FONT_HERSHEY_SIMPLEX, 3, (0, 0, 0), 3)

        # Add overlay text for shot result if it exists
        if hasattr(self, 'overlay_text'):
            # Calculate text size to position it at the right top corner
            (text_width, text_height), _ = cv2.getTextSize(self.overlay_text, cv2.FONT_HERSHEY_SIMPLEX, 3, 6)
            text_x = self.frame.shape[1] - text_width - 40  # Right alignment with some margin
            text_y = 100  # Top margin

            # Display overlay text with color (overlay_color)
            cv2.putText(self.frame, self.overlay_text, (text_x, text_y), cv2.FONT_HERSHEY_SIMPLEX, 3,
                        self.overlay_color, 6)
            # cv2.putText(self.frame, self.overlay_text, (text_x, text_y), cv2.FONT_HERSHEY_SIMPLEX, 3, (0, 0, 0), 3)

        # Gradually fade out color after shot
        if self.fade_counter > 0:
            alpha = 0.2 * (self.fade_counter / self.fade_frames)
            self.frame = cv2.addWeighted(self.frame, 1 - alpha, np.full_like(self.frame, self.overlay_color), alpha, 0)
            self.fade_counter -= 1


if __name__ == "__main__":
    ShotDetector()

