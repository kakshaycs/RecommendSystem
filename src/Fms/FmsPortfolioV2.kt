package com.syfe.backend.account.services.portfolio

import com.syfe.backend.account.enums.PortfolioDetailsAPIVersion
import com.syfe.backend.account.enums.TransactionType
import com.syfe.backend.account.services.FmsDataTransformationService
import com.syfe.backend.account.services.FmsService
import com.syfe.backend.geoData.GeoContext
import com.syfe.backend.geoData.portfolio.interfaces.IPortfolioService
import com.syfe.backend.portfolios.services.DepositReminderService
import java.math.BigDecimal

class FmsPortfolioV2(
    private val fmsService: FmsService,
    private val fmsDataTransformationService: FmsDataTransformationService,
    private val depositReminderService: DepositReminderService,
    private val portfolioDetailsService: PortfolioDetailsService,
    private val geoContext: GeoContext,
    private val geoPortfolioService: IPortfolioService,
    private val commons: FmsPortfolioCommons,
    portfolioData: PortfolioData
) : FmsPortfolio(
    fmsService,
    fmsDataTransformationService,
    geoContext,
    geoPortfolioService,
    portfolioDetailsService,
    depositReminderService,
    commons,
    PortfolioDetailsAPIVersion.IS_V2,
    portfolioData
) {

    override fun getPortfolio(): MutableMap<String, Any?> {
        val portfolioMap = mutableMapOf<String, Any?>()
        val initialData = fetchInitialPortfolioData(portfolioMap)
        val portfolioTransactions = initialData["portfolioTransactions"] as List<Map<String, Any?>>
        insertBasicPortfolioKeys(portfolioMap, pricingPlan, portfolioTransactions)

        portfolioMap.putAll(
            mapOf(
                "dividends" to portfolioTransactions.filter { it["type"] == TransactionType.CASH_DIVIDEND }.map {
                    mapOf(
                        "securityName" to it["securityName"],
                        "amount" to it["amount"],
                        "date" to it["date"]
                    )
                },
            )
        )

        val totalManagementFees = mngnmtFeesAndTotal?.totalFee ?: BigDecimal.ZERO
        mgmtFees = mngnmtFeesAndTotal?.transactions!!

        portfolioMap.putAll(
            mapOf(
                "mgmtFees" to mgmtFees,
                "totalMgmtFee" to totalManagementFees.toString(),
            )
        )

        return portfolioMap
    }
}
