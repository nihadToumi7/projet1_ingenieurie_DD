import csv
from collections import defaultdict
from pydantic import BaseModel
from typing import Dict, List
import json

# --- 1. MODÈLE PYDANTIC ---
# Ce modèle définit la structure de ton fichier CSV final
class DeptSummary(BaseModel):
    departement: str
    conformite_eau_moyenne: float = 0.0
    nombre_deces: int = 0
    population_totale: int = 0
    nb_cancers: int = 0
    nb_insuffisance_renale: int = 0
    nb_foie_pancreas: int = 0
    pourcentage_cancer: float = 0.0
    pourcentage_insuffisance_renale: float = 0.0
    pourcentage_foie_pancreas: float = 0.0

# --- 2. FONCTION DE NORMALISATION ---
def normalize_dept(code: str) -> str:
    c = str(code).strip().replace('"', '')
    if not c or c.lower() == "total": return "INCONNU"
    if c in ("99", "999"): return "999"
    if len(c) == 3 and c.startswith('0'): return c[1:]
    if len(c) == 1 and c.isdigit(): return c.zfill(2)
    return c

# --- 3. LECTURE DES DONNÉES ---

def read_water_file(file_path: str) -> Dict[str, float]:
    water_map = defaultdict(list)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                dept = normalize_dept(row.get('cddept', ''))
                cols = [
                    row.get('plvconformitebacterio'), 
                    row.get('plvconformitechimique'),
                    row.get('plvconformitereferencebact'), 
                    row.get('plvconformitereferencechim')
                ]
                # Calcul de conformité (on ignore les 'S' qui sont des non-réalisés)
                valid_cols = [c for c in cols if c != 'S' and c is not None]
                if not valid_cols: continue
                val = (valid_cols.count('C') / len(valid_cols)) * 100
                water_map[dept].append(val)
    except: pass
    return {d: sum(v)/len(v) for d, v in water_map.items()}

def read_effectifs_csv(file_path: str) -> Dict[str, Dict[str, int]]:
    # Structure : { "dept": {"pop": 0, "cancer": 0, "rein": 0, "foie": 0} }
    stats_map = defaultdict(lambda: {"pop": 0, "cancer": 0, "rein": 0, "foie": 0})
    
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                # 1. FILTRES : Année 2023 + Global (tous sexes/âges)
                annee = str(row.get('annee', ''))
                sexe = str(row.get('libelle_sexe', '')).lower()
                age = str(row.get('libelle_classe_age', '')).lower()
                
                if '2023' not in annee: continue
                if 'tous sexes' not in sexe: continue
                if 'tous âges' not in age: continue
                
                dept_raw = str(row.get('dept', ''))
                if 'tous départements' in dept_raw.lower(): continue
                dept = normalize_dept(dept_raw)

               
                p2 = str(row.get('patho_niv1', ''))
                try:
                    count = int(float(row.get('Ntop', 0)))
                except: count = 0

                # 3. RÉPARTITION DANS LES COLONNES SPÉCIFIQUES
                if "Total consommants tous régimes" in p2:
                    stats_map[dept]["pop"] += count
                elif "Cancers" in p2:
                    stats_map[dept]["cancer"] += count
                elif "Insuffisance rénale chronique terminale" in p2:
                    stats_map[dept]["rein"] += count
                elif "Maladies du foie ou du pancréas" in p2:
                    stats_map[dept]["foie"] += count
    except Exception as e:
        print(f"Erreur lors de la lecture des effectifs : {e}")
        
    return stats_map

def read_death_data(file_path: str) -> Dict[str, int]:
    death_map = defaultdict(int)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if len(line) < 167: continue
                code_com = line[162:167].strip()
                if code_com.startswith('97'): raw_dept = code_com[:3]
                elif code_com.startswith('99'): raw_dept = "999"
                else: raw_dept = code_com[:2]
                death_map[normalize_dept(raw_dept)] += 1
    except: pass
    return death_map

# --- 4. EXPORT FINAL ---

def main():
    FILE_WATER = "../DIS_PLV_2023.txt"
    FILE_EFFECTIFS = "../effectifs.csv"
    FILE_DECES = "../deces-2023.txt"

    print("Lecture des fichiers en cours...")
    water = read_water_file(FILE_WATER)
    pathos = read_effectifs_csv(FILE_EFFECTIFS)
    deaths = read_death_data(FILE_DECES)

    all_depts = set(water.keys()) | set(pathos.keys()) | set(deaths.keys())
    
    final_dataset = []
    for d in sorted(all_depts):
        if d == "INCONNU": continue
        
        s = pathos.get(d, {"pop": 0, "cancer": 0, "rein": 0, "foie": 0})
        pop = s["pop"]
        
        # Calcul des pourcentages avec sécurité anti-division par zéro
        p_cancer = (s["cancer"] / pop * 100) if pop > 0 else 0
        p_rein = (s["rein"] / pop * 100) if pop > 0 else 0
        p_foie = (s["foie"] / pop * 100) if pop > 0 else 0
            
        summary = DeptSummary(
            departement=d,
            conformite_eau_moyenne=round(water.get(d, 0.0), 2),
            nombre_deces=deaths.get(d, 0),
            population_totale=pop,
            nb_cancers=s["cancer"],
            nb_insuffisance_renale=s["rein"],
            nb_foie_pancreas=s["foie"],
            # Nouveaux champs :
            pourcentage_cancer=round(p_cancer, 4),
            pourcentage_insuffisance_renale=round(p_rein, 4),
            pourcentage_foie_pancreas=round(p_foie, 4)
        )
        final_dataset.append(summary.model_dump())
    # Exportation CSV
    headers = [
        "departement", "conformite_eau_moyenne", "nombre_deces", 
        "population_totale", "nb_cancers", "nb_insuffisance_renale", 
        "nb_foie_pancreas", "pourcentage_cancer", 
        "pourcentage_insuffisance_renale", "pourcentage_foie_pancreas"
    ]
    with open("analyse_eau_sante_2023.csv", 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(final_dataset)

    print(f"Succès ! Fichier 'analyse_eau_sante_2023.csv' généré avec {len(final_dataset)} départements.")
   
    
    with open("schema_donnees.json", "w", encoding="utf-8") as f:
        json.dump(DeptSummary.model_json_schema(), f, indent=4, ensure_ascii=False)
    
    print("Schéma JSON généré sous le nom 'schema_donnees.json'")

if __name__ == "__main__":
    main()