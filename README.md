# Cns [![Build Status](https://travis-ci.com/hernanirvaz/cns.svg?branch=master)](https://travis-ci.com/hernanirvaz/cns)

Arquiva transactions etherscan/greymass/bitcoinde/kraken/paymium/therock no bigquery. Pode ajustar dias para reposicionamento temporal.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'cns'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install cns

## Usage

    $ cns help [COMMAND]  # Describe available commands or one specific command
    $ cns show            # mostra resumo saldos & transacoes
      ops [-v], [--no-v]  # mostra transacoes
          [-t], [--no-t]  # mostra transacoes todas ou somente novas
    $ cns work            # carrega transacoes novas no bigquery

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/hernanirvaz/cns. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](https://github.com/hernanirvaz/cns/blob/master/CODE_OF_CONDUCT.md).


## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Cns project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/hernanirvaz/cns/blob/master/CODE_OF_CONDUCT.md).
