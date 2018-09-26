import click
import os
import sys
import subprocess
import hashlib
import sqlite3
import json
import datetime
import glob
import traceback

def gather_params(args):
    return {args[i].lstrip('-'): args[i+1] for i in range(0, len(args), 2)}

def hash_file(fname):
    md5 = hashlib.md5()

    with open(fname, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()

def latest(what, which):
    """Finds the object corresponding to the latest `what`, identified by `which`"""
    assert what in ["model", "data"]

    try:
        conn = sqlite3.connect("db/experiments.db")
        cur = conn.cursor()

        if what == 'data':
            cur.execute("SELECT fname, version FROM data WHERE key = ? ORDER BY version DESC LIMIT 1", (which,))
            fname = cur.fetchone()
        elif what == 'model':
            cur.execute('SELECT id FROM expmeta WHERE key = ? ORDER BY id DESC LIMIT 1', (which,))
            expid = cur.fetchone()
            if expid is not None:
                expid = expid[0]
            else:
                expid = -1
            cur.execute("SELECT fname FROM expres WHERE exp = ? AND fname IS NOT NULL ORDER BY id DESC LIMIT 1", (expid,))
            fname = cur.fetchone()
        else:
            print("ERROR: don't know what a \"{}\" is".format(what))

        return fname[0] if fname is not None else None

    except Exception as e:
        print("ERROR: Failed to find latest \"{}\" with key \"{}\"".format(what, which))
        traceback.print_exc()
        raise e

@click.group()
def cli():
    pass

@cli.command()
def init():
    """Initializes a new project."""

    try:
        os.mkdir('src')
        os.mkdir('data')
        os.mkdir('db')
        os.mkdir('models')
        os.mkdir('models/logs')

        os.utime('__init__.py', None)
        os.utime('src/__init__.py', None)
        os.utime('data/__init__.py', None)

        conn = sqlite3.connect("db/experiments.db")
        cur = conn.cursor()

        cur.execute("CREATE TABLE data (id INTEGER PRIMARY KEY AUTOINCREMENT, fname TEXT NOT NULL, hash TEXT, version INT NOT NULL DEFAULT(1), key TEXT NOT NULL, params TEXT)")
        cur.execute("CREATE TABLE expmeta (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT, data_key INT, data_ver INT NOT NULL, params TEXT, FOREIGN KEY (data_key) REFERENCES data (id))")
        cur.execute("CREATE TABLE expres (id INTEGER PRIMARY KEY AUTOINCREMENT, exp INT NOT NULL, fname TEXT, epoch INT NOT NULL, result TEXT NOT NULL, FOREIGN KEY (exp) REFERENCES expmeta (id))")
        conn.commit()

        conn.close()
        
    except Exception as e:
        print("ERROR: Could not create project-related directories -- is this folder empty?")
        traceback.print_exc()
        raise e

@cli.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True)
)
@click.argument("key") # exp name
@click.argument("data_key") # data name
@click.pass_context
def run(ctx, key, data_key):
    """Run project. Trailing arguments of the form --key value are passed to the called module."""

    idx = len(glob.glob("models/logs/{}_*.out".format(key)))
    run_str = "{}_{}".format(key, idx)
    sys.stdout = open("models/logs/{}.out".format(run_str), "w")
    sys.stderr = open("models/logs/{}.err".format(run_str), "w")

    try:
        main_args = gather_params(ctx.args)

        conn = sqlite3.connect("db/experiments.db")
        cur = conn.cursor()

        cur.execute("SELECT fname, version FROM data WHERE key = ? ORDER BY version DESC LIMIT 1", (data_key,))
        data_fname, data_ver = cur.fetchone()

        cur.execute('INSERT INTO expmeta (key, data_key, data_ver, params) VALUES (?, ?, ?, ?)', (key, data_key, data_ver, json.dumps(main_args)))
        conn.commit()

        cur.execute("SELECT last_insert_rowid()")
        expid = cur.fetchone()[0]

        dirname = os.path.join("models", run_str)
        savename = os.path.join(dirname, run_str)

        try:
            os.mkdir(dirname)
        except:
            pass
 
        from src import main
        run_gen = main.make_run(os.path.join("data", data_fname), os.path.join("models", key), main_args)

        for stats, model in run_gen:
            save_fname = None
            if model is not None:
                save_fname = savename + "_" + str(stats['epoch']) + ".pkl"
                model.save(save_fname)

            cur.execute("INSERT INTO expres (fname, exp, epoch, result) VALUES (?, ?, ?, ?)", (save_fname, expid, stats["epoch"], stats["stats"]))
            conn.commit()

        conn.close()
    except Exception as e:
        print("ERROR: Failed to run project")
        traceback.print_exc()
        raise e

@cli.command()
@click.argument("what", type=click.Choice(["data", "model"]))
@click.argument("which")
def find(what, which):
    """Finds the latest object with the given tag"""

    print(latest(what, which))

@cli.command()
@click.argument("what", type=click.Choice(["data", "model"]))
def clean(what):
    """Cleanup function."""

    try:
        conn = sqlite3.connect("db/experiments.db")
        cur = conn.cursor()

        it = 'data' if what == 'data' else 'expres'
        cur.execute("SELECT DISTINCT key FROM {}".format(it))
        keys = cur.fetchall()
        keep = {}
        delete = []

        for k in keys:
            cur.execute("SELECT fname FROM {} WHERE key = ? AND fname IS NOT NULL ORDER BY {} DESC LIMIT 1".format(it, 'version' if what == 'data' else 'id'), (k,))
            keep[k] = cur.fetchone()[0]
            cur.execute("SELECT fname FROM {} WHERE key = ? AND fname <> ? AND fname IS NOT NULL".format(it), (k, keep[-1]))
            delete.extend(cur.fetchall())
        
        print("WARNING: The following files will be deleted:")
        for f in delete:
            print("\t{}".format(f))
        do_it = input("Proceed [y/N]? ")
        if do_it.trim().lower() not in ["y", "ye", "yes"]:
            print("Operation cancelled, quitting")
            sys.exit(2)

        for f in delete:
            print("Deleting {}...".format(f))
            os.remove(f)
        for k, f in keep.iteritems():
            cur.execute("DROP * FROM {} WHERE fname <> ? AND key = ? AND fname IS NOT NULL".format(it), (k, f))
        conn.commit()

        conn.close()
            
    except Exception as e:
        print("ERROR: Failed to {} the data (parameters: {})".format(verb, args)) 
        print("Verify that gen.py script exists and that the database was initialized (project init)")
        traceback.print_exc()
        raise e

@cli.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True)
)

@click.argument("verb", type=click.Choice(["check", "gen"]))
@click.argument("args", nargs=-1)
def data(verb, args):
    """Data manipulation functions."""

    try:
        conn = sqlite3.connect("db/experiments.db")
        cur = conn.cursor()

        if verb == "gen":
            # args[0] is the raw dataset, args[1] is the dataset name/keyword
            from data import gen
            cur.execute("SELECT version FROM data WHERE key = ? ORDER BY version DESC LIMIT 1", (args[1],))
            prev_ver = cur.fetchone()

            if prev_ver is None:
                version = 1
            else:
                version = prev_ver[0] + 1

            fname = args[1] + "v{}.data".format(version)

            params = gather_params(args[2:])
            cur.execute("INSERT INTO data (key, fname, version, hash, params) VALUES (?, ?, ?, ?, ?)", (args[1], fname, version, "", json.dumps(params)))
            conn.commit()
            gen.run(args[0], os.path.join("data", fname), params)

            filehash = hash_file(os.path.join("data", fname))
            cur.execute("UPDATE data SET hash = ? WHERE fname = ?", (filehash, fname))
            conn.commit()

        elif verb == "check":
            # args[0] is data to check
            cur.execute("SELECT fname FROM data WHERE key = ?", (args[0],))
            fname = cur.fetchone()[0]
            filehash = hash_file(os.path.join("data", fname))

            cur.execute("SELECT hash FROM data WHERE key = ? ORDER BY version DESC LIMIT 1", (args[0],))
            dbhash = cur.fetchone()[0]
            if dbhash != filehash:
                print("WARNING: Latest experiment data does not match current dataset:\n\tCurrent dataset: {}\n\tLatest recorded dataset: {}".format(filehash, dbhash))
            else:
                print("Current dataset matches latest data")

        else:
            cur.execute("SELECT DISTINCT key FROM data")
            keys = cur.fetchall()
            keep = {}
            delete = []

            for k in keys:
                cur.execute("SELECT fname FROM data WHERE key = ? ORDER BY version DESC LIMIT 1", (k,))
                keep[k] = cur.fetchone()[0]
                cur.execute("SELECT fname FROM data WHERE key = ? AND fname <> ?", (k, keep[-1]))
                delete.extend(cur.fetchall())
            
            print("WARNING: The following files will be deleted:")
            for f in delete:
                print("\t{}".format(f))
            do_it = input("Proceed [y/N]? ")
            if do_it.trim().lower() not in ["y", "ye", "yes"]:
                print("Operation cancelled, quitting")
                sys.exit(2)

            for f in delete:
                print("Deleting {}...".format(f))
                os.remove(f)
            for k, f in keep.iteritems():
                cur.execute("DROP * FROM data WHERE fname <> ? AND key = ?", (k, f))
            conn.commit()

        conn.close()
            
    except Exception as e:
        print("ERROR: Failed to {} the data (parameters: {})".format(verb, args)) 
        print("Verify that gen.py script exists and that the database was initialized (project init)")
        traceback.print_exc()
        raise e

if __name__ == '__main__':
    cli()
