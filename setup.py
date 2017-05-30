from __future__ import unicode_literals

import codecs
import os

from setuptools import find_packages, setup


here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='indico_migrate',
    version='0.0.1',
    description='Migration from Indico 1.2 to 2.0',
    long_description=long_description,
    url='https://github.com/indico/indico-migrate',
    author='Indico Team',
    author_email='indico-team@cern.ch',
    license='https://www.gnu.org/licenses/gpl-3.0.txt',
    packages=find_packages(),
    install_requires=['indico>=1.9.11.dev3'],
    entry_points={
        'console_scripts': [
            'indico-migrate = indico_migrate.cli:main'
        ]
    },
)
