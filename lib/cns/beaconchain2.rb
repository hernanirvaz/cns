# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar historicos da beaconchain
  class Beaconchain
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
