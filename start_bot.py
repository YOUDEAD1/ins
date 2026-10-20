"""Local setup/launcher for LARA STOCK. Never embeds or prints credentials."""
import getpass
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import venv

PROJECT = Path(__file__).resolve().parent


def python_path():
    return PROJECT / '.venv' / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')


def validate(name, value):
    if not value or '\n' in value or '\r' in value:
        return False
    if name == 'TOKEN':
        return bool(re.fullmatch(r'\d+:[A-Za-z0-9_-]{20,}', value))
    if name == 'MONGO_URI':
        return value.startswith(('mongodb://', 'mongodb+srv://'))
    if name == 'OWNER_ID':
        return value.isdigit() and int(value) > 0
    if name == 'OWNER_USER':
        return bool(re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{4,31}', value))
    return False


def configure():
    from dotenv import load_dotenv, set_key
    env_file = PROJECT / '.env'
    load_dotenv(env_file, override=False)
    prompts = {
        'TOKEN': ('Telegram bot token (input hidden): ', True),
        'MONGO_URI': ('MongoDB connection URI (input hidden): ', True),
        'OWNER_ID': ('Your Telegram numeric user ID: ', False),
        'OWNER_USER': ('Support Telegram username (without @): ', False),
    }
    for name, (prompt, hidden) in prompts.items():
        value = os.environ.get(name, '').strip()
        if name == 'OWNER_USER':
            value = value.lstrip('@')
        if validate(name, value):
            os.environ[name] = value
            continue
        while True:
            value = (getpass.getpass(prompt) if hidden else input(prompt)).strip()
            if name == 'OWNER_USER':
                value = value.lstrip('@')
            if validate(name, value):
                break
            print('Invalid value. Check it and enter it again. No value was printed.')
        set_key(str(env_file), name, value, quote_mode='always')
        os.environ[name] = value
        if os.name != 'nt':
            env_file.chmod(0o600)
    os.environ.setdefault('MONGO_DB_NAME', 'shop_db')
    return env_file


def install():
    if sys.version_info < (3, 10):
        raise RuntimeError('Python 3.10 or newer is required; Python 3.12 was used for verification.')
    if not (PROJECT / 'bot.py').is_file():
        raise RuntimeError('Place start_bot.py and START-BOT.cmd beside bot.py and the webapp folder.')
    interpreter = python_path()
    if not interpreter.is_file():
        print('Creating a separate Python environment...', flush=True)
        venv.EnvBuilder(with_pip=True).create(PROJECT / '.venv')
    req = PROJECT / 'requirements-tested.txt'
    if not req.is_file():
        req = PROJECT / 'requirements.txt'
    if not req.is_file():
        raise RuntimeError('requirements.txt is missing.')
    fingerprint = hashlib.sha256(req.read_bytes()).hexdigest()
    stamp = PROJECT / '.venv' / 'lara-requirements.sha256'
    if not stamp.is_file() or stamp.read_text().strip() != fingerprint:
        print('Installing the bot libraries. The first setup can take a few minutes...', flush=True)
        subprocess.run([str(interpreter), '-m', 'pip', 'install', '--disable-pip-version-check', '-r', str(req)],
                       cwd=PROJECT, check=True)
        stamp.write_text(fingerprint + '\n')
    # Configuration runs in the environment where python-dotenv was installed.
    return subprocess.call([str(interpreter), str(Path(__file__).resolve()), '--run'], cwd=PROJECT)


def run():
    configure()
    if not (PROJECT / 'webapp' / 'index.html').is_file():
        print('The webapp folder is missing. The Telegram bot can start, but its website will be unavailable.', flush=True)
    print('\nStarting the real bot. Keep this window open; press Ctrl+C to stop.', flush=True)
    print('If this token is running on Render or another machine, stop that copy first to prevent polling conflicts.', flush=True)
    return subprocess.call([sys.executable, '-u', str(PROJECT / 'bot.py')], cwd=PROJECT, env=os.environ.copy())


def main():
    try:
        return run() if sys.argv[1:] == ['--run'] else install()
    except KeyboardInterrupt:
        print('\nStopped.')
        return 130
    except subprocess.CalledProcessError as exc:
        print(f'Setup failed (exit code {exc.returncode}). Check the library installation messages above.')
        return exc.returncode or 1
    except Exception as exc:
        # Do not print exception text that might contain connection credentials.
        print(f'Startup failed: {type(exc).__name__}. Check the project files, Python installation, and connection settings.')
        return 1


if __name__ == '__main__':
    sys.exit(main())
