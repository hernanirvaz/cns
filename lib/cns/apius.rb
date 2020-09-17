# frozen_string_literal: true

require('openssl')
require('base64')
require('curb')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar dados no kraken
  class Apius
    # @return [String] API key
    attr_reader :aky
    # @return [String] API secret
    attr_reader :asc
    # @return [String] API url base
    attr_reader :urb
    # @return [String] API private path
    attr_reader :pth

    # @param [String] pky API key
    # @param [String] psc API secret
    # @param [Hash] ops parametrizacao base da API
    # @return [Apius] API kraken base
    def initialize(
      pky: ENV['KRAKEN_API_KEY'],
      psc: ENV['KRAKEN_API_SECRET'],
      ops: { www: 'https://api.kraken.com', ver: 0 }
    )
      @aky = pky
      @asc = psc
      @urb = "#{ops[:www]}/#{ops[:ver]}"
      @pth = "/#{ops[:ver]}/private/"
    end

    # @example
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
    # @return [Hash] saldos no kraken
    def account
      api_post('Balance')[:result]
    end

    # @example
    #  {
    #    error: [],
    #    result: {
    #      trades: {
    #        "TVINF5-TIOUB-YFNGKE": {
    #          ordertxid: 'ORPSUW-YKP4F-UJZOC6',
    #          pair: 'XETHXXBT',
    #          time: 1_463_435_684.8387,
    #          type: 'buy',
    #          ordertype: 'market',
    #          price: '0.024989',
    #          cost: '1.193973',
    #          fee: '0.003104',
    #          vol: '47.77994129',
    #          margin: '0.000000',
    #          misc: ''
    #        },
    #        "OUTRO-TRADE-ID": {}
    #      },
    #      count: 157
    #    }
    #  }
    # @param [Integer] ofs offset dos dados a obter
    # @param [Hash] has acumulador dos dados a obter
    # @return [Hash] dados trades no kraken
    def trades(ofs = 0, has = {})
      r = api_post('TradesHistory', ofs: ofs)[:result]
      has.merge!(r[:trades])
      ofs += 50
      ofs < r[:count] ? trades(ofs, has) : has
    rescue StandardError
      has
    end

    # @example
    #  {
    #    error: [],
    #    result: {
    #      ledger: {
    #        "LXXURB-ITI7S-CXVERS": {
    #          refid: 'ACCHF3A-RIBBMO-VYBESY',
    #          time: 1_543_278_716.2775,
    #          type: 'withdrawal',
    #          subtype: '',
    #          aclass: 'currency',
    #          asset: 'ZEUR',
    #          amount: '-15369.6200',
    #          fee: '0.0900',
    #          balance: '0.0062'
    #        },
    #        "OUTRO-LEDGER-ID": {}
    #      },
    #      count: 376
    #    }
    #  }
    # @param (see trades)
    # @return [Hash] dados ledger no kraken
    def ledger(ofs = 0, has = {})
      r = api_post('Ledgers', ofs: ofs)[:result]
      has.merge!(r[:ledger])
      ofs += 50
      ofs < r[:count] ? ledger(ofs, has) : has
    rescue StandardError
      has
    end

    private

    # HTTP GET request for public kraken API queries.
    def api_get(uri, **ops)
      resposta(Curl.get("#{urb}/public/#{uri}", ops))
    end

    # HTTP POST request for private kraken API queries involving user credentials.
    def api_post(uri, **ops)
      # continually-increasing unsigned integer nonce from the current Unix Time
      ops.merge!({ nonce: Integer(Float(Time.now) * 1e6) })

      resposta(Curl.post("#{urb}/private/#{uri}", ops) { |r| r.headers = hdrs(uri, ops) })
    end

    # @return [Hash] headers necessarios para pedido HTTP
    def hdrs(qry, ops)
      {
        'api-key': aky,
        'api-sign': auth(qry, ops[:nonce], URI.encode_www_form(ops))
      }
    end

    # @return [String] assinarura codificada dos pedidos HTTP
    def auth(qry, non, par)
      raise(ArgumentError, 'API Key is not set') unless aky
      raise(ArgumentError, 'API Secret is not set') unless asc

      Base64.strict_encode64(
        OpenSSL::HMAC.digest('sha512', Base64.decode64(asc), [pth, qry, Digest::SHA256.digest("#{non}#{par}")].join)
      )
    end

    # @return [Hash] resposta do pedido HTTP
    def resposta(http)
      http.response_code == 200 ? JSON.parse(http.body, symbolize_names: true) : http.status
    rescue JSON::ParserError,
           EOFError,
           Errno::ECONNRESET,
           Errno::EINVAL,
           Net::HTTPBadResponse,
           Net::HTTPHeaderSyntaxError,
           Net::ProtocolError,
           Timeout::Error => e
      "Erro da API kraken #{e.inspect}"
    end
  end
end
