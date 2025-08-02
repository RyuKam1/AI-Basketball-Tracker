from ultralytics import YOLO  # Add this import
import supervision as sv
import sys 
sys.path.append('../')
from utils import read_stub, save_stub
import torch

class PlayerTracker:
    """
    A class that handles player detection and tracking using YOLO and ByteTrack.

    This class combines YOLO object detection with ByteTrack tracking to maintain consistent
    player identities across frames while processing detections in batches.
    """
    def __init__(self, model_path):
        """
        Initialize the PlayerTracker with YOLO model and ByteTrack tracker.

        Args:
            model_path (str): Path to the YOLO model weights.
        """
        self.model = YOLO(model_path)
        # Force GPU usage if available
        if torch.cuda.is_available():
            self.model.to('cuda')
            # Use mixed precision to save memory
            self.model.fuse()  # Fuse layers for faster inference
        self.tracker = sv.ByteTrack()

    def detect_frames(self, frames):
        """
        Detect players in a sequence of frames using batch processing.

        Args:
            frames (list): List of video frames to process.

        Returns:
            list: YOLO detection results for each frame.
        """
        batch_size = 8  # Reduced from 32 for 4GB VRAM
        detections = [] 
        for i in range(0,len(frames),batch_size):
            detections_batch = self.model.predict(frames[i:i+batch_size],conf=0.5)
            detections += detections_batch
        return detections

    def get_object_tracks(self, frames, read_from_stub=False, stub_path=None):
        """
        Get player tracking results for a sequence of frames with optional caching.

        Args:
            frames (list): List of video frames to process.
            read_from_stub (bool): Whether to attempt reading cached results.
            stub_path (str): Path to the cache file.

        Returns:
            list: List of dictionaries containing player tracking information for each frame,
                where each dictionary maps player IDs to their bounding box coordinates.
        """
        tracks = read_stub(read_from_stub,stub_path)
        if tracks is not None:
            if len(tracks) == len(frames):
                return tracks

        detections = self.detect_frames(frames)

        tracks=[]

        for frame_num, detection in enumerate(detections):
            cls_names = detection.names
            cls_names_inv = {v:k for k,v in cls_names.items()}

            # Covert to supervision Detection format
            detection_supervision = sv.Detections.from_ultralytics(detection)

            # Track Objects
            detection_with_tracks = self.tracker.update_with_detections(detection_supervision)

            tracks.append({})

            for frame_detection in detection_with_tracks:
                bbox = frame_detection[0].tolist()
                cls_id = frame_detection[3]
                track_id = frame_detection[4]

                if cls_id == cls_names_inv['Player']:
                    tracks[frame_num][track_id] = {"bbox":bbox}
        
        save_stub(stub_path,tracks)
        return tracks
