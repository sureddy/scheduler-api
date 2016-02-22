from setuptools import setup
setup(
    name="scheduler",
    packages=["scheduler"],
    package_data={
        'scheduler': ['slurm/scripts/*'],
    }
)
