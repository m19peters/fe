import typer
import json
import runner

app = typer.Typer()

@app.command()
def run(self, name: str = typer.Option(..., "--name", "-n")
        , source_db_name:str = typer.Option(..., "--source-db-name", "-s")
        , source_query: str = typer.Option(..., "--source-query", "-sq")
        , target_db_name:str = typer.Option(..., "--target-db-name", "-t")
        , target_query: str = typer.Option(..., "--target-query", "-tq")
        , duck_file: str = typer.Option("compare", "--duck-file", "-d")
        , threads:int = typer.Option(10, "--threads", "-th")):
    
    r = runner.validation_runner()
    r.run_validation(name,source_db_name,source_query,target_db_name,target_query,duck_file,threads)

@app.command()
def run_config(config_path:str = typer.Option(..., "--config-path", "-c")
               , tag:str = typer.Option("", "--tag", "-t")):
        
    validations = []

    with open(config_path,"r") as f:
        js = json.load(f)

        for c in js["config"]:
            for v in c: # configs
                for i in c[v]: #group of validations in configs
                    if (v.lower()==tag.lower() or not tag):
                        threads = 10
                        duck_file = "compare"
                        run_detail = False

                        if (i.get("threads")):
                            threads = int(i["threads"])
                        if (i.get("duck_file")):
                            duck_file = str(i["duck_file"])
                        if (i.get("run_detail")):
                            run_detail = bool(i["run_detail"])

                        vd = {
                            "name" : i["name"],
                            "source_db_name" : i["source_db_config"],
                            "source_query" : i["source_query"],
                            "target_db_name" : i["target_db_config"],
                            "target_query" : i["target_query"],
                            "duck_file" : duck_file,
                            "threads" : threads,
                            "run_detail" : run_detail
                        }

                        validations.append(vd)
    
    r = runner.validation_runner()

    r.run_validations(validations)                    
    r.upload_validations()

if __name__ == "__main__":
    app()

