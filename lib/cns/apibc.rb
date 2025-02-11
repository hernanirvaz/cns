# frozen_string_literal: true

require("faraday")
require("json")

# @author Hernani Rodrigues Vaz
module Cns
  # classe para acesso dados blockchains
  class Apibc
    # Get account balances for multiple ETH addresses
    # @param addresses [Array<String>] List of ETH addresses (max 20)
    # @return [Array<Hash>] List of addresses with balances
    def account_es(addresses)
      response = etherscan_req("balancemulti", addresses.join(","), 1, tag: "latest")
      response[:status] == "1" ? response.dig(:result) : []
    end

    # Get EOS account information
    # @param address [String] EOS account name
    # @return [Hash] Account details with resources
    def account_gm(address)
      response = greymass_req("/v1/chain/get_account", { account_name: address })
      response || { core_liquid_balance: 0, total_resources: { net_weight: 0, cpu_weight: 0 } }
    end

    # Get normal transactions for ETH address
    # @param [String] address endereco ETH
    # @return [Array<Hash>] lista transacoes normais etherscan
    def norml_es(address)
      pag_etherscan_req("txlist", address)
    end

    # Get internal transactions for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista transacoes internas etherscan
    def inter_es(address)
      pag_etherscan_req("txlistinternal", address)
    end

    # Get mined blocks for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista blocos etherscan
    def block_es(address)
      pag_etherscan_req("getminedblocks", address, blocktype: "blocks")
    end

    # Get withdrawals for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista blocos etherscan
    def withw_es(address)
      pag_etherscan_req("txsBeaconWithdrawal", address)
    end

    # Get token transfers for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista token transfer events etherscan
    def token_es(address)
      pag_etherscan_req("tokentx", address)
    end

    # Get complete transaction history for EOS account
    # @param (see account_gm)
    # @return [Array<Hash>] lista completa transacoes greymass
    def ledger_gm(address)
      actions = []
      pos = 0
      loop do
        response = greymass_req("/v1/history/get_actions", { account_name: address, pos: pos, offset: 100 })
        batch = response[:actions] || []
        actions += batch
        break if batch.size < 100

        pos += 100
      end
      actions
    end

    private

    # Reusable Faraday connection
    def connection(base_url)
      Faraday.new(base_url) do |conn|
        conn.headers = { content_type: "application/json", accept: "application/json", user_agent: "blockchain-api-client" }
        conn.adapter Faraday.default_adapter
      end
    end

    # Generic Etherscan API request handler
    def etherscan_req(action, address, page = 1, params = {})
      params = {
        module: "account",
        action: action,
        address: address,
        page: page,
        apikey: ENV.fetch("ETHERSCAN_API_KEY"),
      }.merge(params)
      parse_json(connection("https://api.etherscan.io").get("/api", params).body)
    rescue Faraday::Error, JSON::ParserError
      { status: "0", result: [] }
    end

    # Generic method for paginated Etherscan requests
    # @param action [String] API action name
    # @param address [String] Blockchain address
    # @param params [Hash] Additional request parameters
    # @return [Array<Hash>] Combined results from all pages
    def pag_etherscan_req(action, address, params = {})
      results = []
      page = 1
      loop do
        response = etherscan_req(action, address, page, params)
        break unless response[:status] == "1"

        batch = response[:result] || []
        results += batch
        break if batch.size < 10000

        page += 1
      end
      results
    end

    # Generic Greymass API request handler
    def greymass_req(endpoint, payload)
      parse_json((connection("https://eos.greymass.com").post(endpoint) { |req| req.body = payload.to_json }).body)
    rescue Faraday::Error, JSON::ParserError
      nil
    end

    # Safe JSON parsing with error handling
    def parse_json(body)
      JSON.parse(body, symbolize_names: true)
    rescue JSON::ParserError
      {}
    end
  end
end
