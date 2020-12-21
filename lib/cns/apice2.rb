# frozen_string_literal: true

require('openssl')
require('base64')
require('curb')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para acesso dados centralized exchanges
  class Apice
    # @example trades_de
    #  {
    #    trades: [{
    #      trade_id: 'XUWWD3',
    #      trading_pair: 'btceur',
    #      is_external_wallet_trade: false,
    #      type: 'sell',
    #      amount_currency_to_trade: '0.1',
    #      price: 430,
    #      volume_currency_to_pay: 43,
    #      volume_currency_to_pay_after_fee: 42.79,
    #      amount_currency_to_trade_after_fee: 0.099,
    #      fee_currency_to_pay: 0.22,
    #      fee_currency_to_trade: '0.00100000',
    #      created_at: '2014-03-22T08:14:48+01:00',
    #      successfully_finished_at: '2014-03-25T14:03:22+01:00',
    #      state: 1,
    #      is_trade_marked_as_paid: true,
    #      trade_marked_as_paid_at: '2014-03-22T08:20:01+01:00',
    #      payment_method: 1,
    #      my_rating_for_trading_partner: 'positive',
    #      trading_partner_information: {
    #        username: 'emax2000',
    #        is_kyc_full: false,
    #        trust_level: 'bronze',
    #        amount_trades: 4,
    #        rating: 100,
    #        bank_name: 'CASSA DI RISPARMIO DI CIVITAVECCHIA SPA',
    #        bic: 'CRFIIT2CXXX',
    #        seat_of_bank: 'IT'
    #      }
    #    }, {}],
    #    page: { current: 1, last: 2 },
    #    errors: [],
    #    credits: 22
    #  }
    # @param [Integer] pag pagina dos dados
    # @param [Array<Hash>] ary acumulador dos dados
    # @param [String] uri Uniform Resource Identifier do pedido HTTP
    # @return [Array<Hash>] lista completa trades bitcoinde
    def trades_de(pag = 0, ary = [], uri = 'https://api.bitcoin.de/v4/trades')
      par = "#{uri}?#{URI.encode_www_form(state: 1, page: pag += 1)}"
      res = JSON.parse(Curl.get(par) { |obj| obj.headers = hde(par) }.body, symbolize_names: true)
      ary += res[:trades]
      rep = res[:page]
      rep[:current] < rep[:last] ? trades_de(pag, ary) : ary
    rescue StandardError
      ary
    end

    # @example deposits_de
    #  {
    #    deposits: [
    #      {
    #        deposit_id: '177245',
    #        txid: '84f9e85bc5709cd471e3d58a7d0f42d2c4a7bbd888cabf844e200efbf0a7fda2',
    #        address: '1KK6HhG3quojFS4CY1mPcbyrjQ8BMDQxmT',
    #        amount: '0.13283',
    #        confirmations: 6,
    #        state: 2,
    #        created_at: '2014-01-31T22:01:30+01:00'
    #      },
    #      {}
    #    ],
    #    page: { current: 1, last: 1 },
    #    errors: [],
    #    credits: 23
    #  }
    # @param (see trades_de)
    # @return [Array<Hash>] lista completa uniformizada depositos bitcoinde
    def deposits_de(pag = 0, ary = [], uri = 'https://api.bitcoin.de/v4/btc/deposits')
      par = "#{uri}?#{URI.encode_www_form(state: 2, page: pag += 1)}"
      res = JSON.parse(Curl.get(par) { |obj| obj.headers = hde(par) }.body, symbolize_names: true)
      ary += res[:deposits].map { |has| deposit_unif(has) }
      rep = res[:page]
      rep[:current] < rep[:last] ? deposits_de(pag, ary) : ary
    rescue StandardError
      ary
    end

    # @example deposit_unif
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
    # @return [Hash] deposit uniformizado bitcoinde
    def deposit_unif(has)
      {
        add: has[:address],
        time: Time.parse(has[:created_at]),
        qt: has[:amount],
        txid: Integer(has[:deposit_id])
      }.merge(tp: 'deposit', moe: 'btc', fee: '0')
    end

    # @example withdrawals_de
    #  {
    #    withdrawals: [
    #      {
    #        withdrawal_id: '136605',
    #        address: '1K9YMDDrmMV25EoYNqi7KUEK57Kn3TCNUJ',
    #        amount: '0.120087',
    #        network_fee: '0',
    #        comment: '',
    #        created_at: '2014-02-05T13:01:09+01:00',
    #        txid: '6264fe528116fcb87c812a306ca8409eecfec8fa941546c86f98984b882c8042',
    #        transferred_at: '2014-02-05T13:05:17+01:00',
    #        state: 1
    #      },
    #      {}
    #    ],
    #    page: { current: 1, last: 2 },
    #    errors: [],
    #    credits: 23
    #  }
    # @param (see deposits_de)
    # @return [Array<Hash>] lista completa uniformizada withdrawals bitcoinde
    def withdrawals_de(pag = 0, ary = [], uri = 'https://api.bitcoin.de/v4/btc/withdrawals')
      par = "#{uri}?#{URI.encode_www_form(state: 1, page: pag += 1)}"
      res = JSON.parse(Curl.get(par) { |obj| obj.headers = hde(par) }.body, symbolize_names: true)
      ary += res[:withdrawals].map { |has| withdrawal_unif(has) }
      rep = res[:page]
      rep[:current] < rep[:last] ? withdrawals_de(pag, ary) : ary
    rescue StandardError
      ary
    end

    # @example withdrawal_unif
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
    # @return [Hash] withdrawal uniformizada bitcoinde
    def withdrawal_unif(has)
      {
        add: has[:address],
        time: Time.parse(has[:transferred_at]),
        qt: has[:amount],
        fee: has[:network_fee],
        txid: Integer(has[:withdrawal_id])
      }.merge(tp: 'withdrawal', moe: 'btc')
    end

    # @example ledger_fr
    #  [
    #    {
    #      uuid: '50551e61-4e74-4ae7-85fd-9c2040542818',
    #      currency_amount: nil,
    #      state: 'executed',
    #      btc_fee: '0.0',
    #      currency_fee: '0.0',
    #      created_at: '2014-03-04T09:00Z',
    #      updated_at: '2014-03-04T09:00Z',
    #      currency: 'EUR',
    #      comment: '5723',
    #      amount: '100.0',
    #      type: 'WireDeposit',
    #      account_operations: [{
    #        uuid: 'b5058a68-cf99-4438-86d3-e773eba418ec',
    #        name: 'wire_deposit',
    #        amount: '100.0',
    #        currency: 'EUR',
    #        created_at: '2014-03-04T09:00Z',
    #        created_at_int: 1_393_923_644,
    #        is_trading_account: false
    #      }, {}]
    #    },
    #    {}
    #  ]
    # @param (see trades_de)
    # @return [Array<Hash>] lista ledger paymium
    def ledger_fr(pag = 0, ary = [], uri = 'https://paymium.com/api/v1/user/orders')
      res = JSON.parse(
        Curl.get(uri, offset: pag) { |obj| obj.headers = hfr("#{uri}?#{URI.encode_www_form(offset: pag)}") }.body,
        symbolize_names: true
      )
      res.empty? ? ary : ledger_fr(pag + res.size, ary + res)
    rescue StandardError
      ary
    end

    # @example ledger_mt
    #  {
    #    transactions: [
    #      {
    #        id: 305_445,
    #        date: '2014-03-06T10:59:13.000Z',
    #        type: 'withdraw',
    #        price: 97.47,
    #        currency: 'EUR',
    #        fund_id: nil,
    #        order_id: nil,
    #        trade_id: nil,
    #        note: 'BOV withdraw',
    #        transfer_detail: nil
    #      },
    #      {}
    #    ],
    #    meta: {
    #      total_count: nil,
    #      first: { page: 1, href: 'https://api.therocktrading.com/v1/transactions?page=1' },
    #      previous: nil,
    #      current: { page: 1, href: 'https://api.therocktrading.com/v1/transactions?page=1' },
    #      next: { page: 2, href: 'https://api.therocktrading.com/v1/transactions?page=2' },
    #      last: nil
    #    }
    #  }
    # @param (see trades_de)
    # @return [Array<Hash>] lista ledger therock
    def ledger_mt(pag = 1, ary = [], uri = 'https://api.therocktrading.com/v1/transactions')
      res = JSON.parse(
        Curl.get(uri, page: pag) { |obj| obj.headers = hmt("#{uri}?#{URI.encode_www_form(page: pag)}") }.body,
        symbolize_names: true
      )[:transactions]
      res.empty? ? ary : ledger_mt(pag + res.size, ary + res)
    rescue StandardError
      ary
    end

    # @example trades_us
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
    # @param [Integer] ofs offset dos dados
    # @param [Hash] has acumulador dos dados
    # @param (see account_us)
    # @return [Hash] dados trades kraken
    def trades_us(ofs = 0, has = {}, urb = 'https://api.kraken.com/0/private')
      uri = 'TradesHistory'
      non = nnc
      res = JSON.parse(
        Curl.post("#{urb}/#{uri}", nonce: non, ofs: ofs) { |obj| obj.headers = hus(uri, nonce: non, ofs: ofs) }.body,
        symbolize_names: true
      )[:result]
      has.merge!(res[:trades])
      (ofs += 50) < res[:count] ? trades_us(ofs, has) : has
    rescue StandardError
      has
    end

    # @example ledger_us
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
    # @param (see trades_us)
    # @return [Hash] dados ledger kraken
    def ledger_us(ofs = 0, has = {}, urb = 'https://api.kraken.com/0/private')
      uri = 'Ledgers'
      non = nnc
      res = JSON.parse(
        Curl.post("#{urb}/#{uri}", nonce: non, ofs: ofs) { |obj| obj.headers = hus(uri, nonce: non, ofs: ofs) }.body,
        symbolize_names: true
      )[:result]
      has.merge!(res[:ledger])
      (ofs += 50) < res[:count] ? ledger_us(ofs, has) : has
    rescue StandardError
      has
    end
  end
end
