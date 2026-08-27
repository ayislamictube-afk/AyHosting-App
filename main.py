import os
import sys
import json
import time
import socket
import sqlite3
import datetime
import threading
import subprocess
import ast
import shutil
import zipfile
import tarfile
import urllib.request
import urllib.parse
from typing import Dict, List, Optional, Any

# Flask & Tools Auto-install
try:
    from flask import Flask, request, jsonify, render_template_string, Response, send_file
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "flask"])
    from flask import Flask, request, jsonify, render_template_string, Response, send_file

try:
    import yt_dlp
    YTDLP_AVAILABLE = True
except ImportError:
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yt-dlp"])
        import yt_dlp
        YTDLP_AVAILABLE = True
    except Exception:
        YTDLP_AVAILABLE = False

# Paths Configuration
APP_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(APP_DIR, "omnihost_data")
WORKSPACE_DIR = os.path.join(DATA_DIR, "workspaces")
DOWNLOADS_DIR = os.path.join(DATA_DIR, "downloads")
DB_PATH = os.path.join(DATA_DIR, "omnihost.db")

os.makedirs(WORKSPACE_DIR, exist_ok=True)
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

STDLIB_MODULES = {
    'abc', 'argparse', 'array', 'ast', 'asyncio', 'base64', 'binascii', 'bisect', 'builtins',
    'bz2', 'calendar', 'cmath', 'cmd', 'code', 'codecs', 'collections', 'colorsys', 'compileall',
    'concurrent', 'configparser', 'contextlib', 'contextvars', 'copy', 'copyreg', 'cProfile',
    'csv', 'ctypes', 'curses', 'dataclasses', 'datetime', 'dbm', 'decimal', 'difflib', 'dis',
    'doctest', 'email', 'encodings', 'enum', 'errno', 'faulthandler', 'fcntl', 'filecmp',
    'fileinput', 'fnmatch', 'fractions', 'ftplib', 'functools', 'gc', 'getopt', 'getpass',
    'gettext', 'glob', 'graphlib', 'grp', 'gzip', 'hashlib', 'heapq', 'hmac', 'html', 'http',
    'imaplib', 'imghdr', 'importlib', 'inspect', 'io', 'ipaddress', 'itertools', 'json',
    'keyword', 'linecache', 'locale', 'logging', 'lzma', 'mailbox', 'mailcap', 'marshal',
    'math', 'mimetypes', 'mmap', 'modulefinder', 'multiprocessing', 'netrc', 'nntplib', 'numbers',
    'operator', 'optparse', 'os', 'pathlib', 'pdb', 'pickle', 'pickletools', 'pipes', 'pkgutil',
    'platform', 'plistlib', 'poplib', 'posix', 'posixpath', 'pprint', 'profile', 'pstats', 'pwd',
    'py_compile', 'pyclbr', 'pydoc', 'queue', 'quopri', 'random', 're', 'readline', 'reprlib',
    'resource', 'rlcompleter', 'runpy', 'sched', 'secrets', 'select', 'selectors', 'shelve',
    'shlex', 'shutil', 'signal', 'site', 'smtpd', 'smtplib', 'sndhdr', 'socket', 'socketserver',
    'spwd', 'sqlite3', 'ssl', 'stat', 'statistics', 'string', 'stringprep', 'struct', 'subprocess',
    'sunau', 'symbol', 'symtable', 'sys', 'sysconfig', 'syslog', 'tabnanny', 'tarfile', 'telnetlib',
    'tempfile', 'termios', 'test', 'textwrap', 'threading', 'time', 'timeit', 'tkinter', 'token',
    'tokenize', 'tomllib', 'trace', 'traceback', 'tracemalloc', 'tty', 'turtle', 'types', 'typing',
    'unicodedata', 'unittest', 'urllib', 'uu', 'uuid', 'venv', 'warnings', 'wave', 'weakref',
    'webbrowser', 'wsgiref', 'xdrlib', 'xml', 'xmlrpc', 'zipapp', 'zipfile', 'zipimport', 'zlib', '_thread'
}

def get_lan_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = '127.0.0.1'
    finally:
        s.close()
    return ip

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS instances (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                filename TEXT NOT NULL,
                code TEXT,
                language TEXT DEFAULT 'python',
                port INTEGER,
                status TEXT DEFAULT 'STOPPED',
                pid INTEGER,
                auto_restart INTEGER DEFAULT 1,
                restart_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 5,
                env_vars TEXT DEFAULT '{}',
                entry_file TEXT,
                is_archive INTEGER DEFAULT 0,
                archive_dir TEXT,
                last_error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                level TEXT DEFAULT 'INFO',
                message TEXT NOT NULL,
                FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        defaults = {
            'port_range_start': '8000',
            'port_range_end': '8090',
            'global_env': '{}'
        }
        for k, v in defaults.items():
            cursor.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (k, v))
        conn.commit()

init_db()

class ProcessSupervisor:
    def __init__(self):
        self.processes: Dict[str, subprocess.Popen] = {}
        self.logs_buffer: Dict[str, List[Dict[str, Any]]] = {}
        self.event_subscribers: List[Any] = []
        self.lock = threading.Lock()
        self.running = True
        self.start_time = time.time()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        threading.Thread(target=self._boot_recovery, daemon=True).start()

    def _boot_recovery(self):
        time.sleep(1.5)
        with get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT id, name, auto_restart, status FROM instances')
            rows = [dict(r) for r in c.fetchall()]
        for inst in rows:
            if inst['auto_restart'] or inst['status'] == 'RUNNING':
                self.start_instance(inst['id'])

    def add_log(self, instance_id: str, message: str, level: str = 'INFO'):
        timestamp = datetime.datetime.now().strftime('%H:%M:%S')
        log_entry = {'timestamp': timestamp, 'level': level, 'message': message}
        with self.lock:
            if instance_id not in self.logs_buffer:
                self.logs_buffer[instance_id] = []
            self.logs_buffer[instance_id].append(log_entry)
            if len(self.logs_buffer[instance_id]) > 500:
                self.logs_buffer[instance_id].pop(0)
        try:
            with get_db() as conn:
                conn.execute(
                    'INSERT INTO logs (instance_id, timestamp, level, message) VALUES (?, ?, ?, ?)',
                    (instance_id, timestamp, level, message)
                )
                conn.commit()
        except Exception:
            pass
        self.emit_event('log', {'instance_id': instance_id, 'log': log_entry})

    def emit_event(self, event_type: str, data: Any):
        payload = json.dumps({'type': event_type, 'data': data})
        with self.lock:
            for queue in list(self.event_subscribers):
                try:
                    queue.append(payload)
                except Exception:
                    pass

    def allocate_port(self, desired_port: Optional[int] = None) -> int:
        with get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT value FROM settings WHERE key = "port_range_start"')
            start_p = int(c.fetchone()[0] or 8000)
            c.execute('SELECT value FROM settings WHERE key = "port_range_end"')
            end_p = int(c.fetchone()[0] or 8090)
            c.execute('SELECT port FROM instances WHERE status = "RUNNING"')
            busy_ports = {row[0] for row in c.fetchall() if row[0]}

        def is_free(p: int) -> bool:
            if p in busy_ports:
                return False
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.4)
                return s.connect_ex(('127.0.0.1', p)) != 0

        if desired_port and start_p <= desired_port <= end_p and is_free(desired_port):
            return desired_port
        for p in range(start_p, end_p + 1):
            if is_free(p):
                return p
        return start_p

    def start_instance(self, instance_id: str) -> Dict[str, Any]:
        with get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM instances WHERE id = ?', (instance_id,))
            row = c.fetchone()
            if not row:
                return {'success': False, 'error': 'Instance not found'}
            inst = dict(row)
            port = inst['port'] or self.allocate_port()
            c.execute('SELECT value FROM settings WHERE key = "global_env"')
            g_env = json.loads(c.fetchone()[0] or '{}')

        inst_env = json.loads(inst['env_vars'] or '{}')
        env = os.environ.copy()
        env['PORT'] = str(port)
        env['PYTHONUNBUFFERED'] = '1'
        env.update(g_env)
        env.update(inst_env)

        inst_dir = os.path.join(WORKSPACE_DIR, instance_id)
        os.makedirs(inst_dir, exist_ok=True)

        if inst['is_archive']:
            exec_dir = inst['archive_dir'] or inst_dir
            entry = inst['entry_file'] or 'main.py'
            script_path = os.path.join(exec_dir, entry)
        else:
            filename = inst['filename'] or "script.py"
            script_path = os.path.join(inst_dir, filename)
            if inst['code'] is not None:
                with open(script_path, 'w', encoding='utf-8') as f:
                    f.write(inst['code'])
            exec_dir = inst_dir

        lang = (inst['language'] or 'python').lower()
        if lang == 'javascript' or script_path.endswith('.js'):
            cmd = ['node', script_path]
        elif lang == 'bash' or script_path.endswith('.sh'):
            cmd = ['bash', script_path]
        else:
            cmd = [sys.executable, '-u', script_path]

        try:
            self.stop_instance(instance_id, update_db=False)
            proc = subprocess.Popen(
                cmd,
                cwd=exec_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            with self.lock:
                self.processes[instance_id] = proc

            threading.Thread(target=self._stream_pipe, args=(instance_id, proc.stdout, 'INFO'), daemon=True).start()
            threading.Thread(target=self._stream_pipe, args=(instance_id, proc.stderr, 'ERROR'), daemon=True).start()

            with get_db() as conn:
                conn.execute(
                    'UPDATE instances SET status = "RUNNING", pid = ?, port = ?, last_error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                    (proc.pid, port, instance_id)
                )
                conn.commit()

            self.add_log(instance_id, f"🚀 সার্ভিস চালু হয়েছে (PID: {proc.pid}) পোর্ট: {port}", "SUCCESS")
            self.emit_event('status_change', {'instance_id': instance_id, 'status': 'RUNNING', 'pid': proc.pid, 'port': port, 'name': inst['name']})
            return {'success': True, 'pid': proc.pid, 'port': port}
        except Exception as e:
            err_msg = str(e)
            self.add_log(instance_id, f"❌ চালুকরণে ত্রুটি: {err_msg}", "ERROR")
            with get_db() as conn:
                conn.execute('UPDATE instances SET status = "CRASHED", last_error = ? WHERE id = ?', (err_msg, instance_id))
                conn.commit()
            return {'success': False, 'error': err_msg}

    def _stream_pipe(self, instance_id: str, pipe, level: str):
        if not pipe: return
        try:
            for line in iter(pipe.readline, ''):
                if line:
                    clean = line.rstrip('\r\n')
                    lvl = 'ERROR' if level == 'ERROR' or 'traceback' in clean.lower() or 'error' in clean.lower() else 'INFO'
                    self.add_log(instance_id, clean, lvl)
        except Exception:
            pass
        finally:
            try: pipe.close()
            except Exception: pass

    def stop_instance(self, instance_id: str, update_db: bool = True) -> Dict[str, Any]:
        with self.lock:
            proc = self.processes.pop(instance_id, None)
        if proc:
            try:
                proc.terminate()
                try: proc.wait(timeout=1.5)
                except subprocess.TimeoutExpired: proc.kill()
            except Exception:
                pass
        if update_db:
            with get_db() as conn:
                conn.execute('UPDATE instances SET status = "STOPPED", pid = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?', (instance_id,))
                conn.commit()
            self.add_log(instance_id, "⏹ সার্ভিস বন্ধ করা হয়েছে।", "WARN")
            self.emit_event('status_change', {'instance_id': instance_id, 'status': 'STOPPED', 'pid': None})
        return {'success': True}

    def restart_instance(self, instance_id: str) -> Dict[str, Any]:
        self.stop_instance(instance_id)
        time.sleep(0.4)
        return self.start_instance(instance_id)

    def _monitor_loop(self):
        while self.running:
            time.sleep(1.0)
            with get_db() as conn:
                c = conn.cursor()
                c.execute('SELECT * FROM instances WHERE status = "RUNNING"')
                running_rows = [dict(r) for r in c.fetchall()]

            for inst in running_rows:
                inst_id = inst['id']
                proc = None
                with self.lock:
                    proc = self.processes.get(inst_id)
                if proc:
                    poll = proc.poll()
                    if poll is not None:
                        exit_code = poll
                        with self.lock: self.processes.pop(inst_id, None)
                        is_crash = exit_code != 0
                        status = 'CRASHED' if is_crash else 'STOPPED'
                        err_text = f"Process exited with code {exit_code}" if is_crash else "Process stopped."
                        self.add_log(inst_id, f"⚠️ {err_text}", "ERROR" if is_crash else "INFO")

                        auto_restart = bool(inst['auto_restart'])
                        retries = inst['restart_count']
                        max_r = inst['max_retries']

                        with get_db() as db_conn:
                            if is_crash and auto_restart and retries < max_r:
                                new_count = retries + 1
                                db_conn.execute('UPDATE instances SET status = "RESTARTING", restart_count = ?, last_error = ? WHERE id = ?', (new_count, err_text, inst_id))
                                db_conn.commit()
                                threading.Thread(target=self._delayed_restart, args=(inst_id, 2.0), daemon=True).start()
                            else:
                                db_conn.execute('UPDATE instances SET status = ?, pid = NULL, last_error = ? WHERE id = ?', (status, err_text if is_crash else None, inst_id))
                                db_conn.commit()
                        self.emit_event('status_change', {'instance_id': inst_id, 'status': status, 'pid': None})

    def _delayed_restart(self, instance_id: str, delay: float):
        time.sleep(delay)
        self.start_instance(instance_id)

supervisor = ProcessSupervisor()

def detect_python_dependencies(code: str) -> List[str]:
    try: tree = ast.parse(code)
    except Exception: return []
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names: modules.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module: modules.add(node.module.split('.')[0])
    third_party = [m for m in modules if m and m not in STDLIB_MODULES and not m.startswith('_')]
    return sorted(list(set(third_party)))

# Flask App
server = Flask(__name__)
server.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

HTML_DASHBOARD = """
<!DOCTYPE html>
<html lang="bn" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Ay Hosting Pro</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=Hind+Siliguri:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Outfit', 'Hind Siliguri', sans-serif; background-color: #080c14; color: #f8fafc; }
        .glass-panel { background: rgba(19, 30, 54, 0.75); backdrop-filter: blur(16px); border: 1px solid rgba(255, 255, 255, 0.08); }
        .pulse-dot { animation: pulse 2s infinite; }
        @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); } 70% { box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); } 100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); } }
    </style>
</head>
<body class="min-h-screen flex flex-col antialiased select-none pb-8">
    <header class="sticky top-0 z-40 glass-panel border-b border-slate-800 px-4 py-3 flex items-center justify-between shadow-lg">
        <div class="flex items-center space-x-3">
            <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-500 to-purple-600 flex items-center justify-center text-white font-black text-lg">
                <i class="fa-solid fa-bolt"></i>
            </div>
            <div>
                <div class="flex items-center space-x-2">
                    <span class="font-extrabold text-base sm:text-lg bg-clip-text text-transparent bg-gradient-to-r from-cyan-400 to-purple-400">Ay Hosting Pro</span>
                    <span class="text-[10px] px-2 py-0.5 rounded-full bg-cyan-950 text-cyan-400 border border-cyan-800 font-mono font-semibold">Standalone APK</span>
                </div>
                <p class="text-[11px] text-slate-400">24/7 Autonomous Mobile Server & Bot Engine</p>
            </div>
        </div>
        <div class="glass-panel px-3 py-1 rounded-lg border border-slate-700 flex items-center space-x-2 text-xs">
            <span class="w-2 h-2 rounded-full bg-emerald-400 pulse-dot"></span>
            <span class="text-slate-400">LAN:</span>
            <span id="lan-ip" class="font-mono text-cyan-300 font-bold">127.0.0.1</span>
        </div>
    </header>

    <div class="bg-slate-900 border-b border-slate-800 px-4 py-2 flex items-center justify-between overflow-x-auto gap-2">
        <div class="flex space-x-1">
            <button onclick="switchTab('instances')" id="tab-btn-instances" class="px-3 py-1.5 rounded-xl text-xs font-semibold bg-cyan-500/20 text-cyan-300 border border-cyan-500/40">
                <i class="fa-solid fa-layer-group mr-1"></i>হোস্ট করা সার্ভিস
            </button>
            <button onclick="switchTab('deploy')" id="tab-btn-deploy" class="px-3 py-1.5 rounded-xl text-xs font-semibold text-slate-400 hover:text-slate-200">
                <i class="fa-solid fa-rocket mr-1"></i>ডিপ্লয় (Deploy)
            </button>
            <button onclick="switchTab('terminal')" id="tab-btn-terminal" class="px-3 py-1.5 rounded-xl text-xs font-semibold text-slate-400 hover:text-slate-200">
                <i class="fa-solid fa-terminal mr-1"></i>লাইভ লগ্স
            </button>
            <button onclick="switchTab('downloader')" id="tab-btn-downloader" class="px-3 py-1.5 rounded-xl text-xs font-semibold text-slate-400 hover:text-slate-200">
                <i class="fa-solid fa-download mr-1"></i>ভিডিও ডাউনলোডার
            </button>
        </div>
    </div>

    <main class="flex-1 p-3 sm:p-6 max-w-5xl w-full mx-auto space-y-4">
        <section id="tab-instances" class="space-y-4">
            <div id="instances-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
            <div id="instances-empty" class="hidden glass-panel p-8 rounded-2xl text-center space-y-3 border border-dashed border-slate-700">
                <div class="text-3xl text-slate-500"><i class="fa-solid fa-cube"></i></div>
                <h3 class="text-sm font-bold text-slate-300">কোনো সার্ভিস চালু নেই</h3>
                <button onclick="switchTab('deploy')" class="px-4 py-2 rounded-xl bg-gradient-to-r from-cyan-500 to-blue-600 text-black font-bold text-xs shadow">
                    + নতুন কোড হোস্ট করুন
                </button>
            </div>
        </section>

        <section id="tab-deploy" class="hidden space-y-4">
            <div class="glass-panel p-4 sm:p-5 rounded-2xl border border-slate-800 space-y-3">
                <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    <div>
                        <label class="block text-xs text-slate-400 mb-1">সার্ভিস বা বটের নাম</label>
                        <input type="text" id="deploy-name" placeholder="Telegram Echo Bot" class="w-full bg-slate-900 border border-slate-700 rounded-xl px-3 py-2 text-xs text-slate-200 focus:outline-none">
                    </div>
                    <div>
                        <label class="block text-xs text-slate-400 mb-1">ডেমো কোড লোড করুন</label>
                        <select onchange="loadTemplate(this.value)" class="w-full bg-slate-900 border border-slate-700 rounded-xl px-3 py-2 text-xs text-slate-200">
                            <option value="">-- টেমপ্লেট নির্বাচন করুন --</option>
                            <option value="flask">Flask Microservice</option>
                            <option value="telegram">Telegram Polling Bot</option>
                            <option value="discord">Discord Status Worker</option>
                        </select>
                    </div>
                </div>
                <textarea id="deploy-code" rows="12" class="w-full bg-slate-950 p-3 text-xs font-mono text-cyan-200 rounded-xl border border-slate-700 focus:outline-none" placeholder="# আপনার সম্পূর্ণ কোড এখানে পেস্ট করুন..."></textarea>
                <button onclick="submitDeployment()" id="deploy-btn" class="w-full py-2.5 bg-gradient-to-r from-cyan-500 to-emerald-500 text-black font-extrabold text-xs rounded-xl shadow">
                    হোস্টিং চালু করুন (DEPLOY)
                </button>
            </div>
        </section>

        <section id="tab-terminal" class="hidden space-y-3">
            <div class="glass-panel p-4 rounded-2xl border border-slate-800 space-y-2">
                <div class="flex justify-between items-center pb-2 border-b border-slate-800 text-xs">
                    <span class="font-bold text-slate-300">কনসোল লগ্স (Live Stream)</span>
                    <button onclick="clearConsole()" class="px-2 py-1 bg-slate-800 rounded text-slate-400">ক্লিয়ার</button>
                </div>
                <div id="terminal-window" class="h-80 bg-black/90 rounded-xl p-3 overflow-y-auto font-mono text-xs text-slate-300 space-y-1">
                    <div class="text-cyan-400">[Ay Hosting Engine Online] Ready...</div>
                </div>
            </div>
        </section>

        <section id="tab-downloader" class="hidden space-y-3">
            <div class="glass-panel p-4 rounded-2xl border border-slate-800 space-y-3">
                <h3 class="text-sm font-bold text-slate-200">ভিডিও ডাউনলোডার</h3>
                <div class="flex gap-2">
                    <input type="text" id="vd-url" placeholder="ভিডিও লিংক পেস্ট করুন..." class="flex-1 bg-slate-900 border border-slate-700 rounded-xl px-3 py-2 text-xs text-slate-200">
                    <button onclick="fetchVideo()" class="px-4 py-2 bg-emerald-600 text-white font-bold text-xs rounded-xl">তথ্য আনুন</button>
                </div>
                <div id="vd-info" class="hidden space-y-2 pt-2">
                    <h4 id="vd-title" class="text-xs font-bold text-slate-200"></h4>
                    <select id="vd-format" class="w-full bg-slate-900 border border-slate-700 rounded-xl p-2 text-xs"></select>
                    <button onclick="downloadVideo()" class="w-full py-2 bg-gradient-to-r from-cyan-500 to-emerald-500 text-black font-bold text-xs rounded-xl">ডাউনলোড করুন</button>
                    <div id="vd-link" class="hidden text-xs text-emerald-400 p-2 bg-emerald-950/40 rounded-xl"></div>
                </div>
            </div>
        </section>
    </main>

    <div id="toast" class="fixed bottom-4 right-4 z-50 px-4 py-2 rounded-xl text-xs font-bold bg-cyan-950 border border-cyan-500 text-cyan-300 transition-all duration-300 hidden"></div>

    <script>
        function showToast(msg) {
            const t = document.getElementById('toast');
            t.innerText = msg;
            t.classList.remove('hidden');
            setTimeout(() => t.classList.add('hidden'), 3000);
        }

        function switchTab(tabId) {
            ['instances', 'deploy', 'terminal', 'downloader'].forEach(id => {
                document.getElementById(`tab-${id}`).classList.toggle('hidden', id !== tabId);
                const btn = document.getElementById(`tab-btn-${id}`);
                if (btn) btn.className = id === tabId ? 'px-3 py-1.5 rounded-xl text-xs font-semibold bg-cyan-500/20 text-cyan-300 border border-cyan-500/40' : 'px-3 py-1.5 rounded-xl text-xs font-semibold text-slate-400 hover:text-slate-200';
            });
            if (tabId === 'instances') fetchInstances();
        }

        const TEMPLATES = {
            flask: "from flask import Flask, jsonify\\nimport os\\napp = Flask(__name__)\\nPORT = int(os.environ.get('PORT', 8000))\\n@app.route('/')\\ndef home():\\n    return jsonify({'status': 'online', 'server': 'Ay Hosting'})\\nif __name__ == '__main__':\\n    app.run(host='0.0.0.0', port=PORT)",
            telegram: "import time, os\\nprint('🤖 Telegram Bot Worker active and polling...')\\nwhile True:\\n    time.sleep(10)",
            discord: "import time\\nprint('⚡ Discord Bot Status active...')\\nwhile True:\\n    time.sleep(30)"
        };

        function loadTemplate(k) { if(TEMPLATES[k]) document.getElementById('deploy-code').value = TEMPLATES[k].replace(/\\\\n/g, '\\n'); }

        async function submitDeployment() {
            const name = document.getElementById('deploy-name').value || 'Custom Service';
            const code = document.getElementById('deploy-code').value;
            if (!code.trim()) return showToast('কোড পেস্ট করুন!');

            const res = await fetch('/api/deploy/paste', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ name, code, language: 'python', auto_restart: 1 })
            });
            const d = await res.json();
            if (d.success) {
                showToast(`হোস্টিং সফল: ${name}`);
                switchTab('instances');
            } else {
                showToast(`ত্রুটি: ${d.error}`);
            }
        }

        async function fetchInstances() {
            const res = await fetch('/api/instances');
            const list = await res.json();
            const grid = document.getElementById('instances-grid');
            const empty = document.getElementById('instances-empty');
            grid.innerHTML = '';
            if (!list.length) { empty.classList.remove('hidden'); return; }
            empty.classList.add('hidden');

            list.forEach(inst => {
                const card = document.createElement('div');
                card.className = 'glass-panel p-4 rounded-2xl border border-slate-800 space-y-2';
                card.innerHTML = `
                    <div class="flex justify-between items-center">
                        <h4 class="font-bold text-sm text-cyan-300">\${inst.name}</h4>
                        <span class="text-[10px] px-2 py-0.5 rounded-full \${inst.status === 'RUNNING' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' : 'bg-rose-950 text-rose-400 border border-rose-800'}">\${inst.status}</span>
                    </div>
                    <div class="text-xs font-mono text-slate-400">PORT: <a href="http://127.0.0.1:\${inst.port}" target="_blank" class="text-cyan-400 underline font-bold">\${inst.port || '--'}</a></div>
                    <div class="flex gap-2 pt-2 border-t border-slate-800">
                        \${inst.status === 'RUNNING' ? `<button onclick="stopInstance('\${inst.id}')" class="px-3 py-1 bg-rose-600/30 text-rose-300 rounded-lg text-xs font-bold">Stop</button>` : `<button onclick="startInstance('\${inst.id}')" class="px-3 py-1 bg-emerald-600/30 text-emerald-300 rounded-lg text-xs font-bold">Start</button>`}
                        <button onclick="deleteInstance('\${inst.id}')" class="px-3 py-1 bg-slate-800 text-slate-400 hover:text-rose-400 rounded-lg text-xs">Delete</button>
                    </div>
                `;
                grid.appendChild(card);
            });
        }

        async function startInstance(id) { await fetch(`/api/instances/\${id}/start`, {method: 'POST'}); fetchInstances(); }
        async function stopInstance(id) { await fetch(`/api/instances/\${id}/stop`, {method: 'POST'}); fetchInstances(); }
        async function deleteInstance(id) { if(confirm('সার্ভিসটি ডিলিট করতে চান?')) { await fetch(`/api/instances/\${id}`, {method: 'DELETE'}); fetchInstances(); } }

        function appendLogLine(l) {
            const w = document.getElementById('terminal-window');
            const d = document.createElement('div');
            d.innerHTML = `<span class="text-slate-600">[\${l.timestamp}]</span> \${l.message}`;
            w.appendChild(d);
            w.scrollTop = w.scrollHeight;
        }
        function clearConsole() { document.getElementById('terminal-window').innerHTML = ''; }

        let vdUrl = '';
        async function fetchVideo() {
            vdUrl = document.getElementById('vd-url').value.trim();
            const res = await fetch('/api/video/info', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({url: vdUrl}) });
            const d = await res.json();
            if(d.success) {
                document.getElementById('vd-title').innerText = d.title;
                const sel = document.getElementById('vd-format');
                sel.innerHTML = '';
                (d.formats || []).forEach(f => {
                    const opt = document.createElement('option');
                    opt.value = f.format_id;
                    opt.innerText = `\${f.label} (\${f.ext})`;
                    sel.appendChild(opt);
                });
                document.getElementById('vd-info').classList.remove('hidden');
            } else showToast(d.error);
        }

        async function downloadVideo() {
            const format_id = document.getElementById('vd-format').value;
            const res = await fetch('/api/video/download', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({url: vdUrl, format_id}) });
            const d = await res.json();
            if(d.success) {
                const link = document.getElementById('vd-link');
                link.innerHTML = `ডাউনলোড লিংক: <a href="\${d.download_url}" class="underline font-bold" download>\${d.filename}</a>`;
                link.classList.remove('hidden');
            } else showToast(d.error);
        }

        window.addEventListener('DOMContentLoaded', () => {
            fetchInstances();
            const src = new EventSource('/api/events');
            src.onmessage = (e) => {
                const d = JSON.parse(e.data);
                if (d.type === 'log') appendLogLine(d.data.log);
                if (d.type === 'status_change') fetchInstances();
            };
            fetch('/api/system/stats').then(r=>r.json()).then(s => document.getElementById('lan-ip').innerText = s.lan_ip);
        });
    </script>
</body>
</html>
"""

@server.route('/')
def home():
    return render_template_string(HTML_DASHBOARD)

@server.route('/api/instances', methods=['GET'])
def list_instances():
    with get_db() as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM instances ORDER BY created_at DESC')
        rows = [dict(r) for r in c.fetchall()]
    return jsonify(rows)

@server.route('/api/instances/<instance_id>', methods=['DELETE'])
def delete_instance(instance_id):
    supervisor.stop_instance(instance_id)
    with get_db() as conn:
        conn.execute('DELETE FROM instances WHERE id = ?', (instance_id,))
        conn.execute('DELETE FROM logs WHERE instance_id = ?', (instance_id,))
        conn.commit()
    w_dir = os.path.join(WORKSPACE_DIR, instance_id)
    if os.path.exists(w_dir): shutil.rmtree(w_dir, ignore_errors=True)
    return jsonify({'success': True})

@server.route('/api/instances/<instance_id>/start', methods=['POST'])
def start_instance_route(instance_id):
    return jsonify(supervisor.start_instance(instance_id))

@server.route('/api/instances/<instance_id>/stop', methods=['POST'])
def stop_instance_route(instance_id):
    return jsonify(supervisor.stop_instance(instance_id))

@server.route('/api/deploy/paste', methods=['POST'])
def deploy_paste():
    data = request.json or {}
    name = data.get('name', 'Custom Bot')
    code = data.get('code', '')
    inst_id = f"inst_{int(time.time())}_{os.urandom(3).hex()}"
    port = supervisor.allocate_port()

    pkgs = detect_python_dependencies(code)
    for p in pkgs:
        try: subprocess.check_call([sys.executable, "-m", "pip", "install", p])
        except Exception: pass

    with get_db() as conn:
        conn.execute('''
            INSERT INTO instances (id, name, filename, code, language, port, status, auto_restart)
            VALUES (?, ?, 'bot.py', ?, 'python', ?, 'STOPPED', 1)
        ''', (inst_id, name, code, port))
        conn.commit()

    start_res = supervisor.start_instance(inst_id)
    return jsonify({'success': start_res.get('success', False), 'port': port, 'error': start_res.get('error')})

@server.route('/api/system/stats', methods=['GET'])
def system_stats():
    return jsonify({'lan_ip': get_lan_ip()})

@server.route('/api/events')
def sse_events():
    def event_stream():
        q: List[str] = []
        with supervisor.lock: supervisor.event_subscribers.append(q)
        try:
            while True:
                if q: yield f"data: {q.pop(0)}\\n\\n"
                else: time.sleep(0.25)
        except GeneratorExit:
            with supervisor.lock:
                if q in supervisor.event_subscribers: supervisor.event_subscribers.remove(q)
    return Response(event_stream(), mimetype='text/event-stream')

@server.route('/api/video/info', methods=['POST'])
def video_info():
    if not YTDLP_AVAILABLE: return jsonify({'success': False, 'error': 'yt-dlp is not installed.'})
    url = (request.json or {}).get('url', '').strip()
    try:
        with yt_dlp.YoutubeDL({'quiet': True, 'skip_download': True}) as ydl:
            info = ydl.extract_info(url, download=False)
            formats = [{'format_id': f.get('format_id'), 'label': f"{f.get('height')}p" if f.get('height') else 'Standard', 'ext': f.get('ext', 'mp4')} for f in info.get('formats', []) if f.get('vcodec') != 'none']
            return jsonify({'success': True, 'title': info.get('title'), 'formats': formats[:6]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@server.route('/api/video/download', methods=['POST'])
def video_download():
    data = request.json or {}
    url, format_id = data.get('url', ''), data.get('format_id', '')
    job_id = f"vid_{int(time.time())}"
    out_dir = os.path.join(DOWNLOADS_DIR, job_id)
    os.makedirs(out_dir, exist_ok=True)
    try:
        with yt_dlp.YoutubeDL({'quiet': True, 'outtmpl': os.path.join(out_dir, '%(title).50s.%(ext)s'), 'format': format_id or 'best', 'merge_output_format': 'mp4'}) as ydl:
            info = ydl.extract_info(url, download=True)
            fname = os.path.basename(ydl.prepare_filename(info))
            return jsonify({'success': True, 'filename': fname, 'download_url': f"/api/video/file/{job_id}/{urllib.parse.quote(fname)}"})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@server.route('/api/video/file/<job_id>/<path:fname>', methods=['GET'])
def video_file(job_id, fname):
    return send_file(os.path.join(DOWNLOADS_DIR, job_id, fname), as_attachment=True)

# Start Flask in Background Thread
def run_flask():
    server.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

threading.Thread(target=run_flask, daemon=True).start()

# Kivy + Android Native WebView Engine
from kivy.app import App
from kivy.uix.widget import Widget
from kivy.clock import Clock
from kivy.utils import platform

if platform == 'android':
    from jnius import autoclass
    from android.runnable import run_on_ui_thread

    WebView = autoclass('android.webkit.WebView')
    WebViewClient = autoclass('android.webkit.WebViewClient')
    activity = autoclass('org.kivy.android.PythonActivity').mActivity
    LinearLayout = autoclass('android.widget.LinearLayout')
    LayoutParams = autoclass('android.view.ViewGroup$LayoutParams')
    Color = autoclass('android.graphics.Color')

    class AyHostingApp(App):
        def build(self):
            self.init_webview()
            return Widget()

        @run_on_ui_thread
        def init_webview(self):
            self.webview = WebView(activity)
            s = self.webview.getSettings()
            s.setJavaScriptEnabled(True)
            s.setDomStorageEnabled(True)
            s.setAllowFileAccess(True)
            s.setDatabaseEnabled(True)
            s.setUseWideViewPort(True)
            s.setLoadWithOverviewMode(True)
            self.webview.setWebViewClient(WebViewClient())
            self.webview.setBackgroundColor(Color.parseColor("#080c14"))

            layout = LinearLayout(activity)
            layout.setOrientation(LinearLayout.VERTICAL)
            layout.addView(self.webview, LayoutParams(LayoutParams.MATCH_PARENT, LayoutParams.MATCH_PARENT))
            activity.setContentView(layout)
            Clock.schedule_once(lambda dt: self.webview.loadUrl("http://127.0.0.1:5000"), 1.5)

        def on_pause(self):
            # Keeps the server alive in background when screen turns off
            return True

else:
    import webbrowser
    class AyHostingApp(App):
        def build(self):
            Clock.schedule_once(lambda dt: webbrowser.open("http://127.0.0.1:5000"), 1.0)
            return Widget()

if __name__ == '__main__':
    AyHostingApp().run()
