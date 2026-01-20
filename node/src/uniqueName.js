const ADJECTIVE_BASES = [
  "eager",
  "bright",
  "calm",
  "keen",
  "nimble",
  "sly",
  "brave",
  "gentle",
  "proud",
  "vivid",
  "quick",
  "steady",
  "agile",
  "clever",
  "daring",
  "lofty",
  "mellow",
  "noble",
  "rapid",
  "rugged",
  "serene",
  "sharp",
  "swift",
  "tidy",
  "wily",
  "bold",
  "crisp",
  "hardy",
  "jaunty",
  "spry",
];

const ADJECTIVE_PREFIXES = [
  "ultra",
  "mega",
  "hyper",
  "turbo",
  "neon",
  "solar",
  "lunar",
  "stormy",
  "frosty",
  "ember",
];

const NOUN_BASES = [
  "beaver",
  "fox",
  "otter",
  "hawk",
  "tiger",
  "lynx",
  "raven",
  "wolf",
  "badger",
  "heron",
  "falcon",
  "panther",
  "cougar",
  "owl",
  "sparrow",
  "gecko",
  "python",
  "manta",
  "orca",
  "whale",
  "coyote",
  "stag",
  "bison",
  "bear",
  "puma",
  "elk",
  "crane",
  "dragon",
  "squid",
  "yak",
];

const NOUN_SUFFIXES = [
  "ridge",
  "wave",
  "spark",
  "forge",
  "runner",
  "rover",
  "canyon",
  "engine",
  "summit",
  "harbor",
];

const buildPrefixedList = (prefixes, bases) => {
  const list = [];
  for (const prefix of prefixes) {
    for (const base of bases) {
      list.push(`${prefix}-${base}`);
    }
  }
  return list;
};

const buildSuffixedList = (bases, suffixes) => {
  const list = [];
  for (const base of bases) {
    for (const suffix of suffixes) {
      list.push(`${base}-${suffix}`);
    }
  }
  return list;
};

const ADJECTIVES = buildPrefixedList(ADJECTIVE_PREFIXES, ADJECTIVE_BASES);
const NOUNS = buildSuffixedList(NOUN_BASES, NOUN_SUFFIXES);

const pickRandom = (list) => list[Math.floor(Math.random() * list.length)];

export const uniqueName = () => `${pickRandom(ADJECTIVES)}-${pickRandom(NOUNS)}`;
