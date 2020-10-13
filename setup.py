import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("version", "r") as version_file:
    version = version_file.read().strip()

setuptools.setup(
    name="kml_files",
    version=version,
    author="Azamatov Ondir, Krylov Pavel",
    author_email="ondir.azamatov@megafon.ru",
    description="Подроект заключается в построении алгоритма формирования файла слоев для Google Earth Pro с расширением .kml  или .kmz.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="git@msk-hdp-gitlab.megafon.ru:space/smartcapex/kml_files.git",
    packages=["kml_files", "kml_files.config", "kml_files.kml_layers", "kml_files.load_data", "kml_files.processing"],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
)
