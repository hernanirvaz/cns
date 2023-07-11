# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar historicos da beaconchain
  class Beaconchain
    # @return [Apibc] API blockchains
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :bqd
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Boolean] :v (false) mostra saldos?
    # @option pop [Boolean] :t (false) mostra todos saldos ou somente novos?
    # @return [Beaconchain] API beaconchain - processar historico saldos
    def initialize(dad, pop)
      @api = Apibc.new
      @bqd = dad
      @ops = pop
    end

    # @return [Array<Hash>] lista balancos novos
    def nov
      @nov ||= bcd.map { |obc| obc[:bx].select { |obj| idb.include?(itx(obj[:epoch], obj[:validatorindex])) } }.flatten
    end

    # @return [Array<Integer>] lista dos meus validators
    def lax
      @lax ||= bqd[:wb].map { |obj| obj[:id] }
    end

    # @return [Array<Hash>] todos os dados beaconchain - saldos & historico
    def bcd
      @bcd ||= api.data_bc("/api/v1/validator/#{lax.join(',')}").map { |obj| base_bc(obj) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & beaconchain
    def dados
      @dados ||= bqd[:wb].map { |obq| bq_bc(obq, bcd.select { |obc| obq[:id] == obc[:ax] }.first) }
    end

    # @return [Array<Integer>] lista historicos novos
    def idb
      @idb ||= bcd.map { |obc| obc[:bx].map { |obj| itx(obj[:epoch], obj[:validatorindex]) } }.flatten -
               (ops[:t] ? [] : bqd[:nb].map { |obq| obq[:itx] })
    end

    # @param [Integer] intum
    # @param [Integer] intdois
    # @return [Integer] szudzik pairing two integers
    def itx(intum, intdois)
      intum >= intdois ? intum * intum + intum + intdois : intum + intdois * intdois
    end

    # @example
    #  {
    #    activationeligibilityepoch: 0,
    #    activationepoch: 0,
    #    balance: 32_489_497_108,
    #    effectivebalance: 32_000_000_000,
    #    exitepoch: 9_223_372_036_854_775_807,
    #    lastattestationslot: 265_446,
    #    name: '',
    #    pubkey: '0x93bf23a587f11f9eca329a12ef51296b8a9848af8c0fe61201524b14cb85b0c6fbd3e427501cdfa3b28719bd1ed96fff',
    #    slashed: false,
    #    status: 'active_online',
    #    validatorindex: 11_766,
    #    withdrawableepoch: 9_223_372_036_854_775_807,
    #    withdrawalcredentials: '0x004f11be01cb72187715c55d6348c67c5a3880687cd42692306fdbc91ac2da9b'
    #  }
    # @param [Hash] abc account beaconchain
    # @return [Hash] dados beaconchain - index, saldo & historico
    def base_bc(abc)
      acc = abc[:validatorindex]
      {
        ax: acc,
        sl: (abc[:balance].to_d / 10**9).round(10),
        bx: api.data_bc("/api/v1/validator/#{acc}/balancehistory")
      }
    end

    # @param [Hash] wbq wallet bigquery
    # @param abc (see base_bc)
    # @return [Hash] dados juntos bigquery & beaconchain
    def bq_bc(wbq, abc)
      xbq = wbq[:id]
      {
        id: xbq,
        ax: wbq[:ax],
        bs: wbq[:sl],
        bb: bqd[:nb].select { |onb| onb[:iax] == xbq },
        es: abc[:sl],
        eb: abc[:bx]
      }
    end

    # @return [String] texto validadores & saldos historicos
    def mostra_resumo
      return unless dados.count.positive?

      puts("\nindex address                            beaconchain blh     bigquery    blh")
      dados.each { |obj| puts(formata_validador(obj)) }
      mostra_saldos
    end

    # @param [Hash] hjn dados juntos bigquery & beaconchain
    # @return [String] texto formatado dum validador
    def formata_validador(hjn)
      format('%<s1>-5.5s %<s2>-34.34s ', s1: hjn[:id], s2: formata_endereco(hjn[:ax], 34)) + formata_valores(hjn)
    end

    # @param (see formata_validador)
    # @return [String] texto formatado valores dum validador
    def formata_valores(hjn)
      format(
        '%<v1>11.6f %<n1>3i %<v2>12.6f %<n2>6i %<ok>-3s',
        v1: hjn[:es],
        n1: hjn[:eb].count,
        v2: hjn[:bs],
        n2: hjn[:bb].count,
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # @param (see formata_validador)
    # @return [Boolean] validador tem historicos novos(sim=NOK, nao=OK)?
    def ok?(hjn)
      hjn[:bs] == hjn[:es]
    end

    # @example pubkey inicio..fim
    #  0x10f3a0cf0b534c..c033cf32e8a03586
    # @param [String] add chave publica validador
    # @param [Integer] max chars a mostrar
    # @return [String] pubkey formatada
    def formata_endereco(add, max)
      return 'erro' if max < 7

      max -= 2
      ini = Integer(max / 2)
      inf = max % 2
      "#{add[0, ini - 3]}..#{add[-inf - ini - 3..]}"
    end

    # @example
    #  {
    #    balance: 32_489_497_108,
    #    effectivebalance: 32_000_000_000,
    #    epoch: 8296,
    #    validatorindex: 11_766,
    #    week: 5
    #  }
    # @param [Hash] hbh historico beaconchain
    # @return [String] texto formatado historico beaconchain
    def formata_saldos(hbh)
      idx = hbh[:validatorindex]
      epc = hbh[:epoch]
      format(
        '%<vi>5i %<vl>17.6f %<ep>6i %<id>9i',
        vi: idx,
        vl: (hbh[:balance].to_d / (10**9)).round(10),
        ep: epc,
        id: itx(epc, idx)
      )
    end

    # @return [String] texto historico saldos
    def mostra_saldos
      return unless ops[:v] && nov.count.positive?

      puts("\nindex             saldo  epoch       itx")
      sorbx.each { |obj| puts(formata_saldos(obj)) }
    end

    # @return [Array<Hash>] lista ordenada historico saldos
    def sorbx
      nov.sort { |ant, prx| ant[:itx] <=> prx[:itx] }
    end
  end
end
