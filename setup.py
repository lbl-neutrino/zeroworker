from subprocess import check_call
import sys

from setuptools import setup
from setuptools.command.install import install


def C(cmd, **kw):
    check_call(cmd, shell=True, **kw)


def install_liblockfile():
    C('git clone https://github.com/miquels/liblockfile.git')
    # sys.prefix points into the venv
    C(f'./configure --prefix="{sys.prefix}"', cwd='liblockfile')
    C('make -j4', cwd='liblockfile')
    C('make install', cwd='liblockfile')
    C('rm -rf liblockfile')


# https://stackoverflow.com/questions/33168482/compiling-installing-c-executable-using-pythons-setuptools-setup-py
class CustomInstall(install):
    def run(self):
        install_liblockfile()
        super().run()


setup(
    cmdclass={'install': CustomInstall},
    name='zeroworker',
    version='0.0',
    description='Zero-infrastructure batch worker library',
    license='0BSD',
    author='Matt Kramer',
    packages=['zeroworker'],
    scripts=['scripts/zw_fan.py', 'scripts/zw_shutdown.py'],
    install_requires=['pyzmq'],
    python_requires='>=3.9'
)
