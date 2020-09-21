# frozen_string_literal: true

require('faraday')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para acesso dados blockchains
  class Apibc
    # @example account_es
    #  {
    #    status: '1',
    #    message: 'OK',
    #    result: [
    #      { account: '0x...', balance: '4000000000000000000' },
    #      { account: '0x...', balance: '87000000000000000000' }
    #    ]
    #  }
    # @param [String] ads lista enderecos ETH (max 20)
    # @param [Hash] par parametros base do pedido HTTP
    # @return [Array<Hash>] lista enderecos e seus saldos
    def account_es(ads, par = { module: 'account', action: 'balancemulti', tag: 'latest' })
      JSON.parse(
        (conn_es.get('api') do |o|
           o.headers = { content_type: 'application/json', accept: 'application/json', user_agent: 'etherscan;ruby' }
           o.params = par.merge(address: ads.join(','), apikey: ENV['ETHERSCAN_API_KEY'])
         end).body,
        symbolize_names: true
      )[:result]
    rescue StandardError
      []
    end

    # @example account_gm
    #  {
    #    account_name: '...',
    #    head_block_num: 141_391_122,
    #    head_block_time: '2020-09-11T16:05:51.000',
    #    privileged: false,
    #    last_code_update: '1970-01-01T00:00:00.000',
    #    created: '2018-06-09T13:14:37.000',
    #
    #    core_liquid_balance: '1232.0228 EOS',
    #    total_resources: { owner: '...', net_weight: '1000.1142 EOS', cpu_weight: '1000.1144 EOS', ram_bytes: 8148 },
    #
    #    ram_quota: 9548,
    #    net_weight: 10_001_142,
    #    cpu_weight: 10_001_144,
    #    net_limit: { used: 0, available: 1_066_648_346, max: 1_066_648_346 },
    #    cpu_limit: { used: 338, available: 88_498, max: 88_836 },
    #    ram_usage: 3574,
    #    permissions: [
    #      {
    #        perm_name: 'active',
    #        parent: 'owner',
    #        required_auth: {
    #          threshold: 1, keys: [{ key: 'EOS...', weight: 1 }], accounts: [], waits: []
    #        }
    #      },
    #      {
    #        perm_name: 'owner',
    #        parent: '',
    #        required_auth: {
    #          threshold: 1, keys: [{ key: 'EOS...', weight: 1 }], accounts: [], waits: []
    #        }
    #      }
    #    ],
    #    self_delegated_bandwidth: { from: '...', to: '...', net_weight: '1000.1142 EOS', cpu_weight: '1000.1144 EOS' },
    #    refund_request: nil,
    #    voter_info: {
    #      owner: '...',
    #      proxy: '...',
    #      producers: [],
    #      staked: 20_002_286,
    #      last_vote_weight: '17172913021904.12109375000000000',
    #      proxied_vote_weight: '0.00000000000000000',
    #      is_proxy: 0,
    #      flags1: 0,
    #      reserved2: 0,
    #      reserved3: '0.0000 EOS'
    #    },
    #    rex_info: nil
    #  }
    # @param [String] add endereco EOS
    # @param [String] uri Uniform Resource Identifier do pedido HTTP
    # @return [Hash] endereco e seus saldo/recursos
    def account_gm(add, uri = '/v1/chain/get_account')
      JSON.parse(
        conn_gm.post(
          uri, { account_name: add }.to_json, content_type: 'application/json'
        ).body,
        symbolize_names: true
      )
    rescue StandardError
      { core_liquid_balance: 0, total_resources: { net_weight: 0, cpu_weight: 0 } }
    end

    # @example norml_es
    #  {
    #    status: '1',
    #    message: 'OK',
    #    result: [
    #      {
    #        blockNumber: '4984535',
    #        timeStamp: '1517094794',
    #        hash: '0x...',
    #        nonce: '10',
    #        blockHash: '0x...',
    #        transactionIndex: '17',
    #        from: '0x...',
    #        to: '0x...',
    #        value: '52627271000000000000',
    #        gas: '21000',
    #        gasPrice: '19000000000',
    #        isError: '0',
    #        txreceipt_status: '1',
    #        input: '0x',
    #        contractAddress: '',
    #        gasUsed: '21000',
    #        cumulativeGasUsed: '566293',
    #        confirmations: '5848660'
    #      },
    #      {}
    #    ]
    #  }
    # @param [String] add endereco ETH
    # @param [Integer] pag pagina transacoes
    # @param [Array<Hash>] ary acumulador transacoes
    # @param par (see account_es)
    # @return [Array<Hash>] lista completa transacoes etherscan
    def norml_es(add, pag = 0, ary = [], par = { module: 'account', action: 'txlist', offset: 10_000 })
      r = JSON.parse(
        (conn_es.get('api') do |o|
           o.headers = { content_type: 'application/json', accept: 'application/json', user_agent: 'etherscan;ruby' }
           o.params = par.merge(address: add, page: pag + 1, apikey: ENV['ETHERSCAN_API_KEY'])
         end).body,
        symbolize_names: true
      )[:result]
      r.count < 10_000 ? ary + r : norml_es(add, pag + 1, ary + r)
    rescue StandardError
      ary
    end

    # @example token_es
    #  {
    #    status: '1',
    #    message: 'OK',
    #    result: [
    #      {
    #        blockNumber: '3967652',
    #        timeStamp: '1499081515',
    #        hash: '0x registo duplicado com todos os dados iguais',
    #        nonce: '3',
    #        blockHash: '0x00a49e999036dc13dc6c4244bb1d51d3146fe7f00bfb500a7624d82e299c7328',
    #        from: '0xd0a6e6c54dbc68db5db3a091b171a77407ff7ccf',
    #        contractAddress: '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0',
    #        to: '0x...',
    #        value: '0',
    #        tokenName: 'EOS',
    #        tokenSymbol: 'EOS',
    #        tokenDecimal: '18',
    #        transactionIndex: '83',
    #        gas: '173399',
    #        gasPrice: '21000000000',
    #        gasUsed: '173398',
    #        input: 'deprecated',
    #        cumulativeGasUsed: '7484878',
    #        confirmations: '3442641'
    #      },
    #      {}
    #    ]
    #  }
    # @param (see norml_es)
    # @return [Array<Hash>] lista completa token transfer events etherscan
    def token_es(add, pag = 0, ary = [], par = { module: 'account', action: 'tokentx', offset: 10_000 })
      # registos duplicados aparecem em token events (ver exemplo acima)
      # -quando ha erros na blockchain (acho)
      # -quando ha token events identicos no mesmo block (acho)
      r = JSON.parse(
        (conn_es.get('api') do |o|
           o.headers = { content_type: 'application/json', accept: 'application/json', user_agent: 'etherscan;ruby' }
           o.params = par.merge(address: add, page: pag + 1, apikey: ENV['ETHERSCAN_API_KEY'])
         end).body,
        symbolize_names: true
      )[:result]
      r.count < 10_000 ? ary + r : token_es(add, pag + 1, ary + r)
    rescue StandardError
      ary
    end

    # @example ledger_gm
    #  {
    #    actions: [
    #      {
    #        account_action_seq: 964,
    #        action_trace: {
    #          account_ram_deltas: [],
    #          act: {
    #            account: 'voicebestapp',
    #            authorization: [
    #              { actor: 'thetruevoice', permission: 'active' },
    #              { actor: 'voicebestapp', permission: 'active' }
    #            ],
    #            data: { from: 'voicebestapp', memo: '...', quantity: '1.0001 MESSAGE', to: '...' },
    #            hex_data: '...',
    #            name: 'transfer'
    #          },
    #          action_ordinal: 10,
    #          block_num: 141_345_345,
    #          block_time: '2020-09-11T09:44:04.500',
    #          closest_unnotified_ancestor_action_ordinal: 5,
    #          context_free: false,
    #          creator_action_ordinal: 5,
    #          elapsed: 6,
    #          producer_block_id: '...',
    #          receipt: {
    #            abi_sequence: 1,
    #            act_digest: '...',
    #            auth_sequence: [['thetruevoice', 6_778_215], ['voicebestapp', 435_346]],
    #            code_sequence: 1,
    #            global_sequence: 233_283_589_258,
    #            receiver: '...',
    #            recv_sequence: 927
    #          },
    #          receiver: '...',
    #          trx_id: '...'
    #        },
    #        block_num: 141_345_345,
    #        block_time: '2020-09-11T09:44:04.500',
    #        global_action_seq: 233_283_589_258,
    #        irreversible: true
    #      },
    #      {}
    #    ],
    #    head_block_num: 141_721_698,
    #    last_irreversible_block: 141_721_371
    #  }
    # @param add (see account_gm)
    # @param [Integer] pos posicao transacoes
    # @param [Array<Hash>] ary acumulador transacoes
    # @param uri (see account_gm)
    # @return [Array<Hash>] lista completa transacoes greymass
    def ledger_gm(add, pos = 0, ary = [], uri = '/v1/history/get_actions')
      r = JSON.parse(
        conn_gm.post(
          uri, { account_name: add, pos: pos, offset: 100 }.to_json, content_type: 'application/json'
        ).body,
        symbolize_names: true
      )[:actions]
      r.count < 100 ? ary + r : ledger_gm(add, pos + r.count, ary + r)
    rescue StandardError
      ary
    end

    private

    # @return [<Symbol>] adapter for the connection - default :net_http
    def adapter
      @adapter ||= Faraday.default_adapter
    end

    # @return [<Faraday::Connection>] connection object for etherscan
    def conn_es
      @conn_es ||=
        Faraday.new(url: 'https://api.etherscan.io/') do |c|
          c.request(:url_encoded)
          c.adapter(adapter)
        end
    end

    # @return [<Faraday::Connection>] connection object for greymass
    def conn_gm
      @conn_gm ||=
        Faraday.new(url: 'https://eos.greymass.com') do |c|
          c.request(:url_encoded)
          c.adapter(adapter)
        end
    end
  end
end
