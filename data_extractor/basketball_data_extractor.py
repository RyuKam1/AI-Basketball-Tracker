import json
import csv
import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import supervision as sv

class BasketballDataExtractor:
    """
    Extracts and saves structured data from basketball video analysis.
    Supports multiple output formats: JSON, CSV, and Excel.
    """
    
    def __init__(self, output_dir: str = "extracted_data"):
        """
        Initialize the data extractor.
        
        Args:
            output_dir (str): Directory to save extracted data files
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize data storage
        self.player_data = []
        self.ball_data = []
        self.pass_data = []
        self.interception_data = []
        self.speed_data = []
        self.team_possession_data = []
        
    def extract_player_data(self, player_tracks: List[Dict], player_assignment: List[Dict], 
                           ball_aquisition: List[Dict], frame_numbers: List[int]) -> None:
        """
        Extract player tracking and team assignment data.
        
        Args:
            player_tracks: List of player tracking data per frame
            player_assignment: List of team assignments per frame
            ball_aquisition: List of ball possession data per frame
            frame_numbers: List of frame numbers
        """
        for frame_idx, (tracks, assignment, possession) in enumerate(zip(player_tracks, player_assignment, ball_aquisition)):
            frame_num = frame_numbers[frame_idx] if frame_idx < len(frame_numbers) else frame_idx
            
            for player_id, track_info in tracks.items():
                bbox = track_info.get('bbox', [])
                team = assignment.get(player_id, 'Unknown')
                
                # Fix: Handle different possession data formats
                if isinstance(possession, dict):
                    has_ball = player_id in possession
                elif isinstance(possession, list):
                    has_ball = player_id in possession
                else:
                    has_ball = False
                
                player_entry = {
                    'frame': frame_num,
                    'player_id': player_id,
                    'team': team,
                    'x_min': bbox[0] if len(bbox) >= 4 else None,
                    'y_min': bbox[1] if len(bbox) >= 4 else None,
                    'x_max': bbox[2] if len(bbox) >= 4 else None,
                    'y_max': bbox[3] if len(bbox) >= 4 else None,
                    'has_ball': has_ball,
                    'timestamp': frame_num / 30.0  # Assuming 30 FPS
                }
                self.player_data.append(player_entry)
    
    def extract_ball_data(self, ball_tracks: List[Dict], frame_numbers: List[int]) -> None:
        """
        Extract ball tracking data.
        
        Args:
            ball_tracks: List of ball tracking data per frame
            frame_numbers: List of frame numbers
        """
        for frame_idx, ball_info in enumerate(ball_tracks):
            frame_num = frame_numbers[frame_idx] if frame_idx < len(frame_numbers) else frame_idx
            
            if ball_info and 1 in ball_info:  # Ball detected
                bbox = ball_info[1].get('bbox', [])
                ball_entry = {
                    'frame': frame_num,
                    'x_min': bbox[0] if len(bbox) >= 4 else None,
                    'y_min': bbox[1] if len(bbox) >= 4 else None,
                    'x_max': bbox[2] if len(bbox) >= 4 else None,
                    'y_max': bbox[3] if len(bbox) >= 4 else None,
                    'center_x': (bbox[0] + bbox[2]) / 2 if len(bbox) >= 4 else None,
                    'center_y': (bbox[1] + bbox[3]) / 2 if len(bbox) >= 4 else None,
                    'timestamp': frame_num / 30.0
                }
                self.ball_data.append(ball_entry)
    
    def extract_pass_data(self, passes: List[Dict], frame_numbers: List[int]) -> None:
        """
        Extract pass detection data.
        
        Args:
            passes: List of detected passes
            frame_numbers: List of frame numbers
        """
        for pass_info in passes:
            pass_entry = {
                'pass_id': pass_info.get('pass_id', len(self.pass_data)),
                'start_frame': pass_info.get('start_frame'),
                'end_frame': pass_info.get('end_frame'),
                'passer_id': pass_info.get('passer_id'),
                'receiver_id': pass_info.get('receiver_id'),
                'passer_team': pass_info.get('passer_team'),
                'receiver_team': pass_info.get('receiver_team'),
                'pass_type': pass_info.get('pass_type', 'Unknown'),
                'start_timestamp': pass_info.get('start_frame', 0) / 30.0,
                'end_timestamp': pass_info.get('end_frame', 0) / 30.0,
                'duration': (pass_info.get('end_frame', 0) - pass_info.get('start_frame', 0)) / 30.0
            }
            self.pass_data.append(pass_entry)
    
    def extract_interception_data(self, interceptions: List[Dict], frame_numbers: List[int]) -> None:
        """
        Extract interception detection data.
        
        Args:
            interceptions: List of detected interceptions
            frame_numbers: List of frame numbers
        """
        for interception_info in interceptions:
            interception_entry = {
                'interception_id': interception_info.get('interception_id', len(self.interception_data)),
                'frame': interception_info.get('frame'),
                'interceptor_id': interception_info.get('interceptor_id'),
                'interceptor_team': interception_info.get('interceptor_team'),
                'original_team': interception_info.get('original_team'),
                'timestamp': interception_info.get('frame', 0) / 30.0
            }
            self.interception_data.append(interception_entry)
    
    def extract_speed_data(self, player_speed_per_frame: List[Dict], frame_numbers: List[int]) -> None:
        """
        Extract player speed data.
        
        Args:
            player_speed_per_frame: List of player speeds per frame
            frame_numbers: List of frame numbers
        """
        for frame_idx, speed_info in enumerate(player_speed_per_frame):
            frame_num = frame_numbers[frame_idx] if frame_idx < len(frame_numbers) else frame_idx
            
            for player_id, speed in speed_info.items():
                speed_entry = {
                    'frame': frame_num,
                    'player_id': player_id,
                    'speed_mps': speed,  # meters per second
                    'speed_kmh': speed * 3.6,  # kilometers per hour
                    'timestamp': frame_num / 30.0
                }
                self.speed_data.append(speed_entry)
    
    def extract_team_possession_data(self, ball_aquisition: List[Dict], frame_numbers: List[int]) -> None:
        """
        Extract team ball possession data.
        
        Args:
            ball_aquisition: List of ball possession data per frame
            frame_numbers: List of frame numbers
        """
        for frame_idx, possession_info in enumerate(ball_aquisition):
            frame_num = frame_numbers[frame_idx] if frame_idx < len(frame_numbers) else frame_idx
            
            # Count players with ball by team
            team_possession = {}
            for player_id in possession_info:
                # You'll need to get team info from player_assignment
                # This is a simplified version
                team_possession[player_id] = 1
            
            possession_entry = {
                'frame': frame_num,
                'possession_data': team_possession,
                'total_players_with_ball': len(possession_info),
                'timestamp': frame_num / 30.0
            }
            self.team_possession_data.append(possession_entry)
    
    def save_to_json(self, filename: str = None) -> str:
        """
        Save all extracted data to a JSON file.
        
        Args:
            filename (str): Output filename (optional)
            
        Returns:
            str: Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"basketball_analysis_{timestamp}.json"
        
        filepath = os.path.join(self.output_dir, filename)
        
        data = {
            'metadata': {
                'extraction_time': datetime.now().isoformat(),
                'total_frames': len(self.player_data) // 10 if self.player_data else 0,  # Approximate
                'total_players': len(set([p['player_id'] for p in self.player_data])),
                'total_passes': len(self.pass_data),
                'total_interceptions': len(self.interception_data)
            },
            'player_data': self.player_data,
            'ball_data': self.ball_data,
            'pass_data': self.pass_data,
            'interception_data': self.interception_data,
            'speed_data': self.speed_data,
            'team_possession_data': self.team_possession_data
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Data saved to JSON: {filepath}")
        return filepath
    
    def save_to_csv(self, filename_prefix: str = None) -> List[str]:
        """
        Save all extracted data to CSV files (one per data type).
        
        Args:
            filename_prefix (str): Prefix for CSV filenames (optional)
            
        Returns:
            List[str]: List of saved file paths
        """
        if filename_prefix is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_prefix = f"basketball_analysis_{timestamp}"
        
        saved_files = []
        
        # Save each data type to separate CSV files
        data_types = [
            ('player_data', self.player_data),
            ('ball_data', self.ball_data),
            ('pass_data', self.pass_data),
            ('interception_data', self.interception_data),
            ('speed_data', self.speed_data),
            ('team_possession_data', self.team_possession_data)
        ]
        
        for data_type, data in data_types:
            if data:  # Only save if data exists
                filename = f"{filename_prefix}_{data_type}.csv"
                filepath = os.path.join(self.output_dir, filename)
                
                df = pd.DataFrame(data)
                df.to_csv(filepath, index=False)
                saved_files.append(filepath)
                print(f"✅ {data_type} saved to CSV: {filepath}")
        
        return saved_files
    
    def save_to_excel(self, filename: str = None) -> str:
        """
        Save all extracted data to an Excel file with multiple sheets.
        
        Args:
            filename (str): Output filename (optional)
            
        Returns:
            str: Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"basketball_analysis_{timestamp}.xlsx"
        
        filepath = os.path.join(self.output_dir, filename)
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            # Save each data type to separate sheets
            data_types = [
                ('Player Data', self.player_data),
                ('Ball Data', self.ball_data),
                ('Pass Data', self.pass_data),
                ('Interception Data', self.interception_data),
                ('Speed Data', self.speed_data),
                ('Team Possession', self.team_possession_data)
            ]
            
            for sheet_name, data in data_types:
                if data:  # Only save if data exists
                    df = pd.DataFrame(data)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Data saved to Excel: {filepath}")
        return filepath
    
    def generate_summary_report(self) -> Dict[str, Any]:
        """
        Generate a summary report of the extracted data.
        
        Returns:
            Dict[str, Any]: Summary statistics
        """
        summary = {
            'total_frames_analyzed': len(set([p['frame'] for p in self.player_data])) if self.player_data else 0,
            'total_players_detected': len(set([p['player_id'] for p in self.player_data])) if self.player_data else 0,
            'total_ball_detections': len(self.ball_data),
            'total_passes': len(self.pass_data),
            'total_interceptions': len(self.interception_data),
            'total_speed_measurements': len(self.speed_data),
            'analysis_duration_seconds': max([p['timestamp'] for p in self.player_data]) if self.player_data else 0,
            'teams_detected': list(set([p['team'] for p in self.player_data if p['team'] != 'Unknown'])) if self.player_data else []
        }
        
        # Calculate average speeds if speed data exists
        if self.speed_data:
            avg_speeds = {}
            for entry in self.speed_data:
                player_id = entry['player_id']
                if player_id not in avg_speeds:
                    avg_speeds[player_id] = []
                avg_speeds[player_id].append(entry['speed_mps'])
            
            summary['average_player_speeds'] = {
                player_id: sum(speeds) / len(speeds) 
                for player_id, speeds in avg_speeds.items()
            }
        
        return summary 