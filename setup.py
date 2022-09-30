import setuptools

setuptools.setup(
    packages=setuptools.find_packages(exclude=['tests*', 'docs', 'execution_scripts']),
    install_requires=[
        'coverage',
        'gitpython',
        'joblib',
        'numpy',
        'pandas',
        'pytest',
        'scipy',
        'sklearn',
        'sphinx',
        'flake8',
        'sphinx-rtd-theme',
        'autopep8'
    ]
)