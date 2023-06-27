package hopper

type OptFunc func(opts *Options)

type Options struct {
	DBName  string
	Encoder DataEncoder
	Decoder DataDecoder
}

func WithDBName(name string) OptFunc {
	return func(opts *Options) {
		opts.DBName = name
	}
}

func WithDecoder(dec DataDecoder) OptFunc {
	return func(opts *Options) {
		opts.Decoder = dec
	}
}

func WithEncoder(enc DataEncoder) OptFunc {
	return func(opts *Options) {
		opts.Encoder = enc
	}
}
