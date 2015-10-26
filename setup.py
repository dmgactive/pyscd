from setuptools import setup, find_packages

setup(
    name='PySCD',
    version = __import__('pyscd').__version__,
    description=('Slowly Changing Dimension management '
                 'supporting SCD types 1 and 2'),
    url='https://github.com/rtogo/pyscd',
    author='Rafael Santos',
    author_email='rstogo@outlook.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=['pyscd'],
    install_requires=['numpy', 'tables'],
)
