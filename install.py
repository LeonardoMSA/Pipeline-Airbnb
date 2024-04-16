class CheckEssentials:
    def installLibs():
        libraries = ['Kaggle', 'pandas', 'os', 'psycopg2-binary', 'psycopg2']

        for lib in libraries:
            try:
                __import__(lib)
                print(f'Library {lib} already installed')

            except ImportError:
                print(f"Library {lib} not found. Installing...")
                import subprocess
                subprocess.check_call(["pip", "install", lib])
                print("Library installed successfully.")


    def checkAPIJsonWin():
        import os
        import getpass

        username = getpass.getuser()
        #print("Current user:", username)

        kaggle_json_path = 'C:\\Users\\' + username + '\\.kaggle\\kaggle.json'
        if not os.path.exists(kaggle_json_path):
            print("Kaggle API credentials file 'kaggle.json' not found in default location.")
            print("Please make sure to configure your Kaggle API credentials by placing the 'kaggle.json' file in the '~/.kaggle/' directory.")
            print("You can obtain the 'kaggle.json' file by creating a new API token in your Kaggle account settings.")
            return None # Early return
        print("File 'Kaggle.json' found!")