module cabby/mruby-bridge

go 1.27.0

require (
	github.com/anycable/anycable-go v0.0.0
	github.com/mitchellh/go-mruby v0.0.0
)

require github.com/matoous/go-nanoid v1.5.1 // indirect

replace (
	github.com/anycable/anycable-go => ../../reference
	github.com/mitchellh/go-mruby => ../../reference/vendorlib/go-mruby
)
