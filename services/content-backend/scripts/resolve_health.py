import os
import sys
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


def main():
    add_resolve_paths()
    try:
        import DaVinciResolveScript as dvr  # type: ignore
    except Exception:
        print("Failed to import DaVinciResolveScript.")
        print("Checked paths:", ", ".join(sys.path))
        sys.exit(3)

    resolve = dvr.scriptapp("Resolve")
    if resolve is None:
        print("DaVinci Resolve is not running.")
        sys.exit(4)

    project_manager = resolve.GetProjectManager()
    project = project_manager.GetCurrentProject() if project_manager else None
    name = project.GetName() if project else "none"
    print(f"Resolve OK. Current project: {name}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("Resolve health check crashed:")
        traceback.print_exc()
        sys.exit(1)
