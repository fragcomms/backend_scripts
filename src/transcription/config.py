MAP_GLOSSARY = {
    "Generic": [
        "CT-Spawn", "T-Spawn", "CT", "T", "Spawn", "A-Site", "B-Site",
        "A", "B"
    ],
    "Nuke": [
        "Silo", "Marshmallow", "Squeaky", "Vent", "Secret", "Unbreakable",
        "Garage", "Mini", "Heaven", "Rafters", "Blue Box", "Yellow",
        "Hut", "Radio", "Trophy", "Ramp", "Credit", "Africa", "Asia",
        "Tetris", "Rafters", "Outside", "Cross", "Single", "Double", "Hell",
        "Control", "Glaive"
    ],
    "Mirage": [
        "Palace", "Tetris", "Connector", "Jungle", "Ticket", "Catwalk",
        "Underpass", "Delpan", "Bench", "Firebox", "Default", "Window",
    ],
    "Inferno": [
        "Coffins", "Pit", "Apartments", "Boiler", "Banana", "Church",
        "Library", "Graveyard", "Ruins", "Top Mid",
    ],
}

GENERAL_CS2_TERMS = [
    "ping", "planting", "NT", "nice try", "fake", "plant", "defuse", "plan", "bomb",
    "pistols", "eco", "dead", "last one", "guns", "bought", "saving", "buy", "force",
    "CZ", "deagle", "dualies", "five-seven", "glock", "p2k", "p250", "r8", "tec9", "usp",
    "AK", "AUG", "AWP", "AVP", "famas", "auto", "galil", "A1S", "M4", "krieg", "SG", "scout",
    "mac10", "mp5", "mp7", "mp9", "pp", "p90", "ump",
    "mag7", "swag7", "nova", "sawed-off", "XM", "auto shotty", "m249", "negev",
    "bayonet", "bowie", "butterfly", "classic", "falchion", "flip", "gut", "huntsman", "karambit",
    "kukri", "m9", "navaja", "nomad", "paracord", "daggers", "skeleton", "stiletto", "survival",
    "talon", "ursus", "zeus", "knife", "dinked", "rat",
    "one HP", "lit", "low", "fifty", "raging", "rage",
    "one", "two", "three", "four", "all five", "all"
]

PHRASES_TO_BOOST = [
    #"one outside", "two outside", "on A", "on B", "last guy",
    #"still secret", "push outside", "going T-spawn",
]

CS2_PHONETIC_FIXES = {
    "Duke": "Nuke",
    "goaded": "goated",
    "hot": "hut",
    "happen": "heaven",
    "fruit's": "threw",
    "empty": "NT",
    "tea spawn": "T-spawn",
    "ex i'm": "XM",
    "pink": "ping",
    "thing": "ping",
    "too many": "two mini",
    "tea": "T",
    "sea tea": "CT",
    "see tea": "CT",
    "dinged": "dinked",
    "acres": "AKs",
    "swan": "one",
    "reaching": "raging"
}

ASR_MODEL_NAME = "nvidia/parakeet-tdt-0.6b-v3"
LLM_MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-AWQ"
