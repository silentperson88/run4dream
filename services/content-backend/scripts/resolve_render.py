import json
import os
import sys
import time
import traceback


def add_resolve_paths():
    candidates = []
    env_path = os.environ.get("RESOLVE_SCRIPTING_PATH")
    if env_path:
        candidates.append(env_path)
    candidates.extend([
        r"C:\ProgramData\Blackmagic Design\DaVinci Resolve\Support\Developer\Scripting\Modules",
        r"C:\Program Files\Blackmagic Design\DaVinci Resolve\Developer\Scripting\Modules",
    ])
    for path in candidates:
        if path and os.path.isdir(path) and path not in sys.path:
            sys.path.append(path)


def frame_to_timecode(frame, fps):
    if fps <= 0:
        fps = 30
    total_frames = int(max(0, frame))
    frames = total_frames % fps
    seconds = (total_frames // fps) % 60
    minutes = (total_frames // (fps * 60)) % 60
    hours = total_frames // (fps * 3600)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}:{frames:02d}"


def main():
    if len(sys.argv) < 3:
        print("Usage: resolve_render.py <input_json> <output_path>")
        sys.exit(2)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    if not os.path.exists(input_path):
        print("Input JSON not found")
        sys.exit(2)

    with open(input_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    segments = payload.get("segments") or []
    audio_path = payload.get("audioPath") or ""
    width = int(payload.get("width") or 1920)
    height = int(payload.get("height") or 1080)
    fps = int(payload.get("fps") or 30)

    add_resolve_paths()
    try:
        import DaVinciResolveScript as dvr  # type: ignore
    except Exception:
        print("Failed to import DaVinciResolveScript.")
        print("Checked paths:", ", ".join(sys.path))
        print("Set RESOLVE_SCRIPTING_PATH to your Resolve scripting Modules folder if needed.")
        sys.exit(3)

    resolve = dvr.scriptapp("Resolve")
    if resolve is None:
        print("DaVinci Resolve is not running. Please open Resolve and try again.")
        sys.exit(4)

    project_manager = resolve.GetProjectManager()
    project = project_manager.GetCurrentProject()
    if project is None:
        project = project_manager.CreateProject("R4D_News_Auto")
        if project is None:
            print("Failed to create Resolve project.")
            sys.exit(5)

    project.SetSetting("timelineResolutionWidth", str(width))
    project.SetSetting("timelineResolutionHeight", str(height))
    project.SetSetting("timelineFrameRate", str(fps))
    project.SetSetting("timelinePlaybackFrameRate", str(fps))

    media_pool = project.GetMediaPool()
    media_storage = resolve.GetMediaStorage()

    timeline_name = f"news_content_{int(time.time())}"
    timeline = media_pool.CreateEmptyTimeline(timeline_name)
    if timeline is None:
        timeline = project.GetCurrentTimeline()
        if timeline is None:
            print("Failed to create Resolve timeline.")
            sys.exit(6)
    project.SetCurrentTimeline(timeline)

    current_frame = 0

    def import_clip(path):
        items = media_storage.AddItemListToMediaPool([path])
        if isinstance(items, list) and items:
            return items[0]
        return None

    # Place audio at timeline start if provided
    if audio_path and os.path.exists(audio_path):
        audio_item = import_clip(audio_path)
        if audio_item:
            media_pool.AppendToTimeline([
                {
                    "mediaPoolItem": audio_item,
                    "startFrame": 0,
                }
            ])

    for segment in segments:
        path = segment.get("path") or ""
        duration_sec = float(segment.get("durationSec") or 0.0)
        if not path or not os.path.exists(path):
            continue
        frames = max(1, int(round(duration_sec * fps)))
        project.SetSetting("stillDuration", str(frames))
        timeline.SetCurrentTimecode(frame_to_timecode(current_frame, fps))
        item = import_clip(path)
        if item:
            media_pool.AppendToTimeline([item])
        current_frame += frames

    # Configure render settings
    output_dir = os.path.dirname(output_path)
    output_name = os.path.splitext(os.path.basename(output_path))[0]
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    project.DeleteAllRenderJobs()
    project.SetRenderSettings({
        "TargetDir": output_dir,
        "CustomName": output_name,
        "ExportVideo": True,
        "ExportAudio": True,
    })

    project.AddRenderJob()
    project.StartRendering()

    # Wait for render completion
    while project.IsRenderingInProgress():
        time.sleep(1)

    print("Resolve render completed")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("Resolve render crashed:")
        traceback.print_exc()
        sys.exit(1)
