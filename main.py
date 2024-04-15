def main():
    import kaggle_link as kag
    import db as dbPost
    
    kag.kaggle.downloadDataset() # Download dataset
    dbPost.postDatabase.createDatabase() # Create database (Postgres required)


if __name__ == "__main__":
    main()
