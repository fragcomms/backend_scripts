import sys
import shutil
import re
from pathlib import Path
from grpc_tools import protoc

# --- Configuration ---
CUSTOM_PACKAGE = "valve_pbuf"

# --- 1. Root Discovery ---
def find_project_root(start_path: Path) -> Path:
    root_markers = {"src", ".git", "pyproject.toml"}
    current = start_path
    for _ in range(6):
        if any((current / marker).exists() for marker in root_markers):
            return current
        if current.parent == current: break
        current = current.parent
    # Only print if we crash
    print("Error: Could not find project root.", file=sys.stderr)
    sys.exit(1)

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = find_project_root(SCRIPT_PATH.parent)

# --- 2. Paths ---
REPO_ROOT = PROJECT_ROOT / "external" / "protobufs"
PROTO_SRC_DIR = REPO_ROOT / "csgo"
OUTPUT_DIR = SCRIPT_PATH.parent
TEMP_BUILD_DIR = PROJECT_ROOT / "temp_proto_build"

FILES_TO_COMPILE = [
    "cstrike15_gcmessages.proto",
    "gcsdk_gcmessages.proto",
    "engine_gcmessages.proto",
    "steammessages.proto"
]

def needs_rebuild() -> bool:
    """Checks if source protos are newer than generated python files."""
    if not OUTPUT_DIR.exists():
        return True
    
    generated_files = list(OUTPUT_DIR.glob("*_pb2.py"))
    if not generated_files or len(generated_files) < len(FILES_TO_COMPILE):
        return True
        
    oldest_gen_time = min(f.stat().st_mtime for f in generated_files)
    
    for fname in FILES_TO_COMPILE:
        src = PROTO_SRC_DIR / fname
        if not src.exists(): continue
        if src.stat().st_mtime > oldest_gen_time:
            return True
            
    return False

def prepare_file(src_path: Path, dest_path: Path):
    """Injects package and fixes references."""
    try:
        content = src_path.read_text(encoding='utf-8', errors='ignore')
        # Remove existing package
        content = re.sub(r'^package\s+[\w\.]+;', '', content, flags=re.MULTILINE)
        # Inject custom package
        new_content = f'package {CUSTOM_PACKAGE};\n' + content
        # Strip leading dots from types to fix internal references
        new_content = re.sub(r'([ \(\)=])\.([A-Z])', r'\1\2', new_content)
        dest_path.write_text(new_content, encoding='utf-8')
    except Exception as e:
        print(f"Failed to prepare {src_path.name}: {e}", file=sys.stderr)
        sys.exit(1)

def fix_imports(output_dir: Path):
    """Generic import fixer for any generated _pb2.py file."""
    for py_file in output_dir.glob("*_pb2.py"):
        content = py_file.read_text(encoding='utf-8')
        
        # Regex: Change "import x_pb2" to "from . import x_pb2"
        new_content = re.sub(
            r'^import (\w+_pb2) as', 
            r'from . import \1 as', 
            content, 
            flags=re.MULTILINE
        )
        
        if content != new_content:
            py_file.write_text(new_content, encoding='utf-8')

def build(force=False):
    # 1. Compile only if needed
    if force or needs_rebuild():
        if not PROTO_SRC_DIR.exists():
            print(f"Error: Protobuf source not found at: {PROTO_SRC_DIR}", file=sys.stderr)
            sys.exit(1)

        if TEMP_BUILD_DIR.exists(): shutil.rmtree(TEMP_BUILD_DIR)
        TEMP_BUILD_DIR.mkdir(parents=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        for fname in FILES_TO_COMPILE:
            prepare_file(PROTO_SRC_DIR / fname, TEMP_BUILD_DIR / fname)

        include_paths = [str(TEMP_BUILD_DIR), str(REPO_ROOT)]
        proto_files = [str(TEMP_BUILD_DIR / f) for f in FILES_TO_COMPILE]

        command = [
            'grpc_tools.protoc',
            f'-I{include_paths[0]}',
            f'-I{include_paths[1]}',
            f'--python_out={str(OUTPUT_DIR)}',
        ] + proto_files

        exit_code = protoc.main(command)
        shutil.rmtree(TEMP_BUILD_DIR)

        if exit_code != 0:
            print("Error: Protobuf compilation failed.", file=sys.stderr)
            sys.exit(exit_code)

    # 2. Patch imports ALWAYS (Fast & Safe)
    fix_imports(OUTPUT_DIR)

if __name__ == "__main__":
    force_rebuild = "--force" in sys.argv
    build(force=force_rebuild)