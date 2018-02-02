from setuptools import setup, find_packages

# set __version__
__version__ = None
with open("./carry/version.py") as fp:
    exec (fp.read())

# http://setuptools.readthedocs.io/en/latest/setuptools.html
setup(
    name='carry',

    version=__version__,

    description='Carry is an utility ETL(extract-transform-load) tool',
    long_description='',

    url='https://github.com/toaco/carry',

    author='Jeffrey',
    author_email='Jeffrey.S.Teo@gmail.com',

    license='GPL-3.0',

    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',

        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",

        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',

        'Operating System :: OS Independent',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    platforms='any',

    keywords=['ETL', 'extract-transform-load', 'utility'],

    packages=find_packages(exclude=['test*']),

    install_requires=['tqdm', 'pandas', 'sqlalchemy', 'six'],

    python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*',

    entry_points={
        'console_scripts': ['carry=carry.command:main'],
    }
)
