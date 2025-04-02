// npm-check-updates

// There are a lot more configuration options.
// See here: https://github.com/raineorshine/npm-check-updates

module.exports = {
    // Exclude packages matching the given string, wildcard, glob,
    // comma-or-space-delimited list, /regex/, or predicate function.
    reject: [],

    // Exclude package.json versions using comma-or-space-delimited list, /regex/, or predicate function.
    rejectVersion: () => {},

    // Include only package names matching the given string, wildcard, glob,
    // comma-or-space-delimited list, /regex/, or predicate function.
    filter: [],

    // Filter on package version using comma-or-space-delimited list, /regex/, or predicate function.
    filterVersion: () => {},

    // Filters out upgrades based on return values
    filterResults: () => {},
}
