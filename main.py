def main():
    import kaggle_link as kag
    import db as dbPost
    import install as ins

    # Instalar bibliotecas caso nao tenha
    ins.CheckEssentials.installLibs()
    ins.CheckEssentials.checkAPIJsonWin()

    # Baixar o dataset e obter o nome
    dataset_name, columns, file_name = kag.kaggle.downloadDataset()

    # Criar o banco de dados com o nome do dataset e inserir os dados
    dbPost.postDatabase.createDatabase(dataset_name)
    dbPost.postDatabase.insertData(dataset_name, columns, file_name)


if __name__ == "__main__":
    main()
