from setuptools import setup

with open("README.en.rst", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='astraflux',
    version='1.5.0',
    description="AstraFlux Description",
    long_description=long_description,
    include_package_data=True,
    package_data={
        'astraflux.web_ui.views': ['*.html'],
    },
    author='YanPing',
    author_email='zyphhxx@foxmail.com',
    maintainer='YanPing',
    maintainer_email='zyphhxx@foxmail.com',
    license='MIT License',
    url='https://github.com/ZYPGITA/astraflux',
    packages=[
        'astraflux',
        'astraflux.boot',
        'astraflux.config',
        'astraflux.controllers',
        'astraflux.core',
        'astraflux.exports',
        'astraflux.providers',
        'astraflux.web_ui',
        'astraflux.web_ui.views',
    ],
    keywords=["distributed", "microservice", "task-queue", "rpc", "scheduler", "agent", "AI"],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    python_requires=">=3.9",
    install_requires=[
        'pika',
        'dill',
        'PyYAML',
        'pymongo',
        'redis',
        'psutil',
        'flask',
        'openai',
        'openpyxl',
        'tomli',
        'tomli_w',
        "requests",
        'flask_cors'
    ]
)
