# Passos para subir a biblioteca 

 - Atualizar arquivo CHANGELOG.txt e setup.py com as mudanças, principalmente de versão
 - No terminal, estando no diretório da pasta pyspark_supp, execute:
    - python3 setup.py sdist
    - twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
    - Colocando o usuário e senha a biblioteca deve ser atualizada no portal Pypi.