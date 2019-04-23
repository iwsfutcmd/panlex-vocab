# PanLex Language Picker

Zero dependency Web Component that allows users to select a PanLex language variety

## Installation

npm install panlex-language-picker

Then add the following tag to your HTML:
`<script type="module" src="node_modules/panlex-language-picker/index.js"></script>`

That's it.

## Usage

The Javascript file defines a new custom element:
`<panlex-language-picker></panlex-language-picker>`

It has no attributes that need to be set. When a language is selected by the user, the `data-lv` attribute is set to the PanLex Language Variety id (e.g. "187" for English), and the `data-uid` attribute is set to the PanLex Language Variety UID (e.g. "eng-000" for English), which can then be read as needed.

### Why is it so ugly?

It's styled as minimally as necessary to make it work, on the principle that implementers will style it themselves. This is also why it's not implemented with a shadow-dom, so external stylesheets will be able to take effect.

## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## History

1.0.0: Initial release

## License

MIT Licensed, see license file.
