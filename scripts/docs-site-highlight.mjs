import { escapeHtml } from "./docs-site-render.mjs";

export function highlight(lang, source) {
  const normalized = (lang || "").toLowerCase();
  if (normalized === "bash" || normalized === "sh" || normalized === "shell") {
    return tokenize(source, BASH_RULES);
  }
  if (normalized === "json") return tokenize(source, JSON_RULES);
  if (normalized === "sql") return tokenize(source, SQL_RULES);
  return escapeHtml(source);
}

function tokenize(source, rules) {
  let out = "";
  let pending = "";
  let i = 0;
  outer: while (i < source.length) {
    for (const [klass, regex] of rules) {
      regex.lastIndex = i;
      const match = regex.exec(source);
      if (match && match.index === i) {
        if (pending) {
          out += escapeHtml(pending);
          pending = "";
        }
        out += `<span class="hl-${klass}">${escapeHtml(match[0])}</span>`;
        i = regex.lastIndex;
        continue outer;
      }
    }
    pending += source[i];
    i += 1;
  }
  if (pending) out += escapeHtml(pending);
  return out;
}

const BASH_RULES = [
  ["c", /#[^\n]*/y],
  ["s", /"(?:\\[\s\S]|[^"\\])*"/y],
  ["s", /'[^'\n]*'/y],
  ["v", /\$\{[^}\n]+\}|\$[A-Za-z_][A-Za-z0-9_]*|\$[0-9?#@*!$-]/y],
  ["f", /(?<=^|[\s=(\[])--?[A-Za-z][\w-]*/y],
  ["n", /(?<=^|[\s=(:,])\d+(?:\.\d+)?\b/y],
  ["k", /\b(?:if|then|else|elif|fi|while|do|done|for|in|case|esac|function|return|exit|local|export|readonly|set|source|alias|cd|read|exec|trap)\b/y],
];

const JSON_RULES = [
  ["s", /"(?:\\[\s\S]|[^"\\])*"(?=\s*:)/y, "key"],
  ["s", /"(?:\\[\s\S]|[^"\\])*"/y],
  ["n", /-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/y],
  ["k", /\b(?:true|false|null)\b/y],
];

const SQL_RULES = [
  ["c", /--[^\n]*/y],
  ["c", /\/\*[\s\S]*?\*\//y],
  ["s", /'(?:''|[^'\n])*'/y],
  ["n", /\b\d+(?:\.\d+)?\b/y],
  ["k", /\b(?:SELECT|FROM|WHERE|AND|OR|NOT|NULL|IS|IN|LIKE|BETWEEN|JOIN|LEFT|RIGHT|INNER|OUTER|ON|GROUP|BY|ORDER|HAVING|LIMIT|OFFSET|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|INDEX|VIEW|DROP|ALTER|ADD|COLUMN|PRIMARY|KEY|FOREIGN|REFERENCES|UNIQUE|DEFAULT|AS|CASE|WHEN|THEN|ELSE|END|UNION|ALL|DISTINCT|COUNT|SUM|AVG|MIN|MAX|WITH|PRAGMA|VIRTUAL|USING|MATCH)\b/iy],
];
