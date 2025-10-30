from helpers import rJson, wJson, adf_to_dict, get_names_in_dir
import json



if __name__ == "__main__":
    jsonsPath = "jsons/newOPPs"
    fileNames = get_names_in_dir(jsonsPath)
    for fileName in fileNames:
        fpath = f"{jsonsPath}/{fileName}"
        data = rJson(fpath)
        firstActivity = data['firstActivity']
        messageBody = firstActivity['message']['body']
        adfDict = adf_to_dict(messageBody)
        firstActivity['adfDict'] = adfDict
        wJson(data, fpath)