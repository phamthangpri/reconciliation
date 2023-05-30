from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'Shareholders_cleaning'
LONG_DESCRIPTION = 'This package allows to clean history data on mapping between bank and backoffice data'

with open("requirements.txt") as f:
    content = f.readlines()
requirements = [x.strip() for x in content if 'git+' not in x]
# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="shareholders_cleaning", 
        version=VERSION,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=requirements, # add any additional packages that 
        # needs to be installed along with your package. Eg: 'caer'
        
        keywords=['python', 'shareholders_cleaning'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)