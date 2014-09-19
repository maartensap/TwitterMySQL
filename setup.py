from distutils.core import setup
from TwitterMySQL import __version__, __author__, __email__
import io

def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)
    
setup(
    name='TwitterMySQL',
    version=__version__,
    author=__author__,
    author_email=__email__,
    packages=['TwitterMySQL'],
    # package_data={'': ['credentials.txt']},
    url='https://github.com/maarten1709/TwitterMySQL',
    download_url = 'https://github.com/maarten1709/TwitterMySQL/archive/master.zip',
    license='MIT',
    keywords=['twitter','MySQL'],
    description='Wrapper for the Twitter APIs and MySQL',
    long_description=read('README.md'),
    install_requires = ['requests', 'requests_oauthlib', 'TwitterAPI', 'MySQL-python']
)
