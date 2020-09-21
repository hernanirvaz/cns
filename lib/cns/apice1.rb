# frozen_string_literal: true

require('openssl')
require('base64')
require('curb')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  DC = %w[LTC NMC PPC DOGE XRP Linden USD CAD GBP ZEC BCH EURN NOKU FDZ GUSD SEED USDC].freeze

  # (see Apice)
  class Apice
    # @example account_de
    #  {
    #    data: {
    #      balances: {
    #        btc: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        bch: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        btg: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        eth: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        bsv: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        ltc: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' }
    #      },
    #      encrypted_information: { uid: '0y...', bic_short: '0y...', bic_full: '0y...' }
    #    },
    #    errors: [],
    #    credits: 23
    #  }
    # @param [String] uri Uniform Resource Identifier do pedido HTTP
    # @return [Hash] saldos no bitcoinde
    def account_de(uri = 'https://api.bitcoin.de/v4/account')
      JSON.parse(
        Curl.get(uri) { |r| r.headers = hde(uri) }.body,
        symbolize_names: true
      )[:data][:balances]
    rescue StandardError
      {}
    end

    # @example account_fr
    #  {
    #    name: '...',
    #    email: '...',
    #    locale: 'en',
    #    channel_id: '...',
    #    meta_state: 'approved',
    #    balance_eur: '0.0',
    #    locked_eur: '0.0',
    #    balance_btc: '0.0',
    #    locked_btc: '0.0',
    #    balance_lbtc: '0.0',
    #    locked_lbtc: '0.0'
    #  }
    # @param (see account_de)
    # @return [Hash] saldos no paymium
    def account_fr(uri = 'https://paymium.com/api/v1/user')
      JSON.parse(
        Curl.get(uri) { |h| h.headers = hfr(uri) }.body,
        symbolize_names: true
      )
    rescue StandardError
      {}
    end

    # @example account_mt
    #  {
    #    balances: [
    #      { currency: 'BTC', balance: 0.0, trading_balance: 0.0 },
    #      { currency: 'ETH', balance: 0.0, trading_balance: 0.0 },
    #      { currency: 'EUR', balance: 0.0, trading_balance: 0.0 },
    #      { currency: 'DAI', balance: 0.0, trading_balance: 0.0 },
    #    ]
    #  }
    # @param (see account_de)
    # @return [Array<Hash>] lista saldos no therock
    def account_mt(uri = 'https://api.therocktrading.com/v1/balances')
      JSON.parse(
        Curl.get(uri) { |h| h.headers = hmt(uri) }.body,
        symbolize_names: true
      )[:balances]
          .delete_if { |e| DC.include?(e[:currency]) }
          .sort { |a, b| a[:currency] <=> b[:currency] }
    rescue StandardError
      []
    end

    # @example account_us
    #  {
    #   error: [],
    #   result: {
    #     ZEUR: '0.0038',
    #     XXBT: '0.0000000000',
    #     XETH: '1.0000000000',
    #     XETC: '0.0000000000',
    #     EOS: '0.0000001700',
    #     BCH: '0.0000000000'
    #   }
    #  }
    # @param [String] urb Uniform Resource Base do pedido HTTP
    # @param uri (see account_de)
    # @param non (see hde)
    # @return [Hash] saldos no kraken
    def account_us(urb = 'https://api.kraken.com/0/private', uri = 'Balance', non = nnc)
      JSON.parse(
        Curl.post("#{urb}/#{uri}", nonce: non) { |h| h.headers = hus(uri, nonce: non) }.body,
        symbolize_names: true
      )[:result]
    rescue StandardError
      {}
    end

    private

    # @example deposits_unif_de
    #  [
    #    {
    #      txid: 177_245,
    #      time: '2014-01-31T22:01:30+01:00',
    #      tp: 'deposit',
    #      add: '1KK6HhG3quojFS4CY1mPcbyrjQ8BMDQxmT',
    #      qt: '0.13283',
    #      moe: 'btc',
    #      fee: '0'
    #    },
    #    {}
    #  ]
    # @param [Hash] hde pagina depositos bitcoinde
    # @return [Array<Hash>] lista uniformizada pagina depositos bitcoinde
    def deposits_unif_de(hde)
      hde[:deposits].map do |h|
        {
          add: h[:address],
          time: Time.parse(h[:created_at]),
          qt: h[:amount],
          txid: Integer(h[:deposit_id])
        }.merge(tp: 'deposit', moe: 'btc', fee: '0')
      end
    end

    # @example withdrawals_unif_de
    #  [
    #    {
    #      txid: 136_605,
    #      time: '2014-02-05T13:05:17+01:00',
    #      tp: 'withdrawal',
    #      add: '1K9YMDDrmMV25EoYNqi7KUEK57Kn3TCNUJ',
    #      qt: '0.120087',
    #      fee: '0',
    #      moe: 'btc'
    #    },
    #    {}
    #  ]
    # @param [Hash] hwi pagina withdrawals bitcoinde
    # @return [Array<Hash>] lista uniformizada pagina withdrawals bitcoinde
    def withdrawals_unif_de(hwi)
      hwi[:withdrawals].map do |h|
        {
          add: h[:address],
          time: Time.parse(h[:transferred_at]),
          qt: h[:amount],
          fee: h[:network_fee],
          txid: Integer(h[:withdrawal_id])
        }.merge(tp: 'withdrawal', moe: 'btc')
      end
    end

    # @return [Integer] continually-increasing unsigned integer nonce from the current Unix Time
    def nnc
      Integer(Float(Time.now) * 1e6)
    end

    # @param [String] qry query a incluir no pedido HTTP
    # @param [Integer] non continually-increasing unsigned integer
    # @return [Hash] headers necessarios para pedido HTTP da exchange bitcoinde
    def hde(qry, non = nnc)
      {
        'X-API-KEY': ENV['BITCOINDE_API_KEY'],
        'X-API-NONCE': non,
        'X-API-SIGNATURE': OpenSSL::HMAC.hexdigest(
          'sha256',
          ENV['BITCOINDE_API_SECRET'],
          ['GET', qry, ENV['BITCOINDE_API_KEY'], non, Digest::MD5.hexdigest('')].join('#')
        )
      }
    end

    # @param (see hde)
    # @return [Hash] headers necessarios para pedido HTTP da exchange paymium
    def hfr(qry, non = nnc)
      {
        content_type: 'application/json',
        'Api-Key': ENV['PAYMIUM_API_KEY'],
        'Api-Nonce': non,
        'Api-Signature': OpenSSL::HMAC.hexdigest('sha256', ENV['PAYMIUM_API_SECRET'], [non, qry].join)
      }
    end

    # @param (see hde)
    # @return [Hash] headers necessarios para pedido HTTP da exchange therock
    def hmt(qry, non = nnc)
      {
        content_type: 'application/json',
        'X-TRT-KEY': ENV['THEROCK_API_KEY'],
        'X-TRT-NONCE': non,
        'X-TRT-SIGN': OpenSSL::HMAC.hexdigest('sha512', ENV['THEROCK_API_SECRET'], [non, qry].join)
      }
    end

    # @param qry (see hde)
    # @param [Hash] ops opcoes trabalho
    # @option ops [Hash] :nonce continually-increasing unsigned integer
    # @return [Hash] headers necessarios para pedido HTTP da exchange kraken
    def hus(qry, ops)
      {
        'api-key': ENV['KRAKEN_API_KEY'],
        'api-sign': Base64.strict_encode64(
          OpenSSL::HMAC.digest(
            'sha512',
            Base64.decode64(ENV['KRAKEN_API_SECRET']),
            ['/0/private/', qry, Digest::SHA256.digest("#{ops[:nonce]}#{URI.encode_www_form(ops)}")].join
          )
        )
      }
    end
  end
end
