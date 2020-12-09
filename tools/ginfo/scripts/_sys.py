"""
Installs the required packages from python env
and system-specific packages.
"""
from subprocess import check_call
from platform import system
import os
import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).resolve().parents[2].__str__())
check_call([sys.executable, '-m', 'pip', 'install', '-r', str(Path('__file__').resolve().parents[3]
                                                              / 'requirements.txt')])
commands = []
if system() == 'Linux':
    os.chdir(Path('/tmp'))
    commands = [
        'wget https://chromedriver.storage.googleapis.com/2.37/chromedriver_linux64.zip',
        'unzip chromedriver_linux64.zip',
        'sudo mv chromedriver /usr/bin/chromedriver',
        'chromedriver --version',
        'curl https://intoli.com/install-google-chrome.sh | bash',
        'sudo mv /usr/bin/google-chrome-stable /usr/bin/google-chrome',
        'google-chrome --version && which google-chrome',
        'sudo yum install poppler-utils'
    ]
if commands:
    for command in commands:
        check_call(command, shell=True)
