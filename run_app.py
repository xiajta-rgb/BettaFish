import sys
import os

venv_path = r"c:\Users\xmls\Documents\trae_projects\BettaFish\venv"

# 添加 venv 根目录（loguru 等可能直接安装在这里）
if venv_path not in sys.path:
    sys.path.insert(0, venv_path)

# 也尝试标准的 Lib/site-packages
venv_site_packages = os.path.join(venv_path, "Lib", "site-packages")
if os.path.exists(venv_site_packages) and venv_site_packages not in sys.path:
    sys.path.insert(0, venv_site_packages)

project_root = r"c:\Users\xmls\Documents\trae_projects\BettaFish"
sys.path.insert(0, project_root)

os.chdir(project_root)

from app import app, socketio

if __name__ == '__main__':
    from config import settings
    HOST = settings.HOST
    PORT = settings.PORT
    
    print(f"Starting BettaFish on http://{HOST}:{PORT}")
    socketio.run(app, host=HOST, port=PORT, debug=False)
