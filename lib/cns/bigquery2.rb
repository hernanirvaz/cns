# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # (see Bigquery)
  class Bigquery
    # @return [Etherscan] API etherscan
    def apies
      @apies ||= Etherscan.new(
        {
          wb: sql("select * from #{BD}.walletEth order by 2"),
          nt: sql("select itx,iax from #{BD}.ethtx"),
          nk: sql("select itx,iax from #{BD}.ethkx")
        },
        ops
      )
    end

    # @return [Greymass] API greymass
    def apigm
      @apigm ||= Greymass.new(
        {
          wb: sql("select * from #{BD}.walletEos order by 2"),
          nt: sql("select itx,iax from #{BD}.eostx")
        },
        ops
      )
    end

    # @return [Kraken] API kraken - obter saldos & transacoes trades e ledger
    def apius
      @apius ||= Kraken.new(
        {
          sl: sql("select sum(btc) xxbt,sum(eth) xeth,sum(eos) eos,sum(eur) zeur from #{BD}.ussl")[0],
          nt: sql("select * from #{BD}.ustx order by time,txid"),
          nl: sql("select * from #{BD}.uslx order by time,txid")
        },
        ops
      )
    end

    # @return [Bitcoinde] API Bitcoinde - obter saldos & transacoes trades e ledger
    def apide
      @apide ||= Bitcoinde.new(
        {
          sl: sql("select sum(btc) btc from #{BD}.desl")[0],
          nt: sql("select * from #{BD}.detx order by time,txid"),
          nl: sql("select * from #{BD}.delx order by time,txid")
        },
        ops
      )
    end

    # @return [Paymium] API Paymium - obter saldos & transacoes ledger
    def apifr
      @apifr ||= Paymium.new(
        {
          sl: sql("select sum(btc) btc,sum(eur) eur from #{BD}.frsl")[0],
          nl: sql("select * from #{BD}.frlx order by time,txid")
        },
        ops
      )
    end

    # @return [TheRock] API TheRock - obter saldos & transacoes ledger
    def apimt
      @apimt ||= TheRock.new(
        {
          sl: sql("select sum(btc) btc,sum(eur) eur from #{BD}.mtsl")[0],
          nl: sql("select * from #{BD}.mtlx order by time,txid")
        },
        ops
      )
    end
  end
end