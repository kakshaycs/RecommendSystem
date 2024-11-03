package com.syfe.backend.promo.services

import com.syfe.backend.account.services.FmsInvestorService
import com.syfe.backend.activeTrading.services.AlfredPromoService
import com.syfe.backend.auth.enums.CampaignType
import com.syfe.backend.auth.enums.EligibleClientType
import com.syfe.backend.auth.enums.FulfillmentRule
import com.syfe.backend.auth.enums.OnboardingFlowType
import com.syfe.backend.auth.enums.PromoTargetProductType
import com.syfe.backend.auth.enums.PromoType
import com.syfe.backend.auth.models.User
import com.syfe.backend.auth.models.UserRepository
import com.syfe.backend.auth.services.UserService
import com.syfe.backend.common.dtos.convert
import com.syfe.backend.common.dtos.objectMapper
import com.syfe.backend.common.enums.AlertType
import com.syfe.backend.common.enums.AuditAction
import com.syfe.backend.common.enums.ServiceType
import com.syfe.backend.common.services.AuditService
import com.syfe.backend.common.services.ColumnEditRecord
import com.syfe.backend.common.services.LocalizationService
import com.syfe.backend.common.utils.addDays
import com.syfe.backend.common.utils.critical
import com.syfe.backend.common.utils.isNullOrBlank
import com.syfe.backend.common.utils.toLocalDate
import com.syfe.backend.geoData.GeoContext
import com.syfe.backend.geoData.account.dtos.ReferralDetails
import com.syfe.backend.geoData.enums.Geography
import com.syfe.backend.geoData.portfolio.interfaces.IPortfolioService
import com.syfe.backend.geoData.promo.constants.IPromoConstants
import com.syfe.backend.onboarding.enums.AmlStatus
import com.syfe.backend.onboarding.enums.Stage
import com.syfe.backend.onboarding.interfaces.IKycUtils
import com.syfe.backend.onboarding.models.TransitionRepository
import com.syfe.backend.portfolios.enums.PortfolioType
import com.syfe.backend.portfolios.exceptions.CustomPromoNotFoundException
import com.syfe.backend.portfolios.services.PortfolioService
import com.syfe.backend.promo.constants.BASE_PROMOTION_CODE
import com.syfe.backend.promo.constants.SPECIAL_ASX_PROMO_CODES
import com.syfe.backend.promo.enums.CustomPromoStatus
import com.syfe.backend.promo.enums.CustomPromoType
import com.syfe.backend.promo.enums.ProductType
import com.syfe.backend.promo.enums.PromoStatus
import com.syfe.backend.promo.enums.RuleEngineType
import com.syfe.backend.promo.enums.UserPromoHistoryStatus
import com.syfe.backend.promo.enums.UserPromoRewardStatus
import com.syfe.backend.promo.enums.UserPromoStatus
import com.syfe.backend.promo.exceptions.InvalidUpdateUserPromoRequestException
import com.syfe.backend.promo.exceptions.InvalidUserPromoException
import com.syfe.backend.promo.exceptions.OwnReferralCodeUseException
import com.syfe.backend.promo.exceptions.PromoCapExceededException
import com.syfe.backend.promo.exceptions.PromoCodeAlreadyExistsException
import com.syfe.backend.promo.exceptions.PromoConsumedException
import com.syfe.backend.promo.exceptions.PromoExpiredException
import com.syfe.backend.promo.exceptions.PromoInvalidDeleteException
import com.syfe.backend.promo.exceptions.PromoInvalidException
import com.syfe.backend.promo.exceptions.PromoInvalidExceptionForUnifiedFlow
import com.syfe.backend.promo.exceptions.PromoNotEligibleException
import com.syfe.backend.promo.exceptions.PromoSuspendedException
import com.syfe.backend.promo.exceptions.ReferralCodeAlreadyUsedException
import com.syfe.backend.promo.exceptions.ReferralPeriodExpiredException
import com.syfe.backend.promo.exceptions.StackingNotAllowedException
import com.syfe.backend.promo.isCustomStackablePromo
import com.syfe.backend.promo.models.AvailPromoRequest
import com.syfe.backend.promo.models.CreatePromoDto
import com.syfe.backend.promo.models.CreatePromoRequest
import com.syfe.backend.promo.models.CustomPromo
import com.syfe.backend.promo.models.CustomPromoData
import com.syfe.backend.promo.models.CustomPromoRepository
import com.syfe.backend.promo.models.DuplicatePromoRequest
import com.syfe.backend.promo.models.Promo
import com.syfe.backend.promo.models.PromoCodeMetaUpdateRequest
import com.syfe.backend.promo.models.PromoRepository
import com.syfe.backend.promo.models.ReferralCodeData
import com.syfe.backend.promo.models.ReferralCodeDetails
import com.syfe.backend.promo.models.UpdatePromoRequest
import com.syfe.backend.promo.models.UpdateUserPromoRequest
import com.syfe.backend.promo.models.UserPromo
import com.syfe.backend.promo.models.UserPromoAvailInfo
import com.syfe.backend.promo.models.UserPromoHistoryInfo
import com.syfe.backend.promo.models.UserPromoRepository
import com.syfe.backend.promo.models.UserPromoRewardsRepository
import com.syfe.backend.promo.models.UserSettingsDetails
import com.syfe.backend.promo.models.validForTradePromotion
import com.syfe.backend.promo.models.validForWealthAndTradePromotion
import com.syfe.backend.promo.models.validForWealthPromotion
import com.syfe.backend.promo.validateCustomStackingLogic
import com.syfe.backend.reliableEventsProcessor.enums.TaskName
import com.syfe.backend.reliableEventsProcessor.interfaces.ITaskService
import com.syfe.backend.variables.services.VariableService
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Sort
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.util.*

@Service
class PromoService(
    private val userRepository: UserRepository,
    private val promoRepository: PromoRepository,
    private val userPromoRepository: UserPromoRepository,
    private val userPromoRewardsRepository: UserPromoRewardsRepository,
    private val transitionRepository: TransitionRepository,
    private val userService: UserService,
    private val promoClawbackService: PromoClawbackService,
    private var localizationService: LocalizationService,
    private val fmsInvestorService: FmsInvestorService,
    private val userPromoService: UserPromoService,
    private val auditService: AuditService,
    private val kycUtils: IKycUtils,
    private val geoPromoConstants: IPromoConstants,
    private val customPromoRepository: CustomPromoRepository,
    private val variableService: VariableService,
    private val geoContext: GeoContext,
    private val portfolioService: PortfolioService,
    private val alfredPromoService: AlfredPromoService,
    private val promoUserSettingsService: PromoUserSettingsService,
    private val taskService: ITaskService,
    private val geoPortfolioService: IPortfolioService
) {
    @Value("\${backend.auth.website.base-url}")
    private lateinit var WEBSITE_BASE_URL: String

    private val logger = KotlinLogging.logger {}

    fun availPromoCode(userId: Long, code: String, email: String? = null, referrerSource: String? = null, channel: String? = null): UserPromo {
        val user = userService.getUserById(userId)
        var promo: Promo? = null
        val type = if (isReferralCode(code)) {
            validateReferralCode(userId, code.uppercase())
            PromoType.REFERRAL
        } else {
            promo = getPromo(code)
            validatePromoApplication(user, promo, email)
            promo.type
        }

        val userPromos = ProductType.getAllowedProductTypes().mapNotNull {
            if (isProductTypeValid(it, user, type, promo)) {
                createPromo(CreatePromoDto(code, userId, promo, type, it, referrerSource, channel))
            } else null
        }

        if (userPromos.isEmpty() && user.onboardingFlowType == OnboardingFlowType.UNIFIED) {
            throw PromoInvalidExceptionForUnifiedFlow(localizationService.getMessage("error.promo.invalid_promo_for_unified_flow"))
        }

        if (userPromos.isEmpty()) throw PromoInvalidException()

        return userPromos[0]
    }

    /**
     * This method check if the product type is valid for users and promo type
     */
    fun isProductTypeValid(productType: ProductType, user: User, type: PromoType, promo: Promo? = null): Boolean {
        val isProductTypeValidForUser = when (productType) {
            ProductType.TRADE -> user.isActiveTrading && (type == PromoType.REFERRAL || promo!!.ruleEngine?.validForTradePromotion() ?: false)
            ProductType.WEALTH -> user.isWealth && (type == PromoType.REFERRAL || promo!!.ruleEngine?.validForWealthPromotion() ?: true)
            else -> throw IllegalStateException()
        }
        logger.info { "AVAIL PROMO:- Validity is $isProductTypeValidForUser for product type $productType for User ${user.id}, " }
        return if (isProductTypeValidForUser) {
            promoUserSettingsService.isPromoUserSettingsValid(promo, user, type, productType)
        } else {
            false
        }
    }

    fun createPromo(
        createPromoDto: CreatePromoDto
    ): UserPromo {
        logger.info { "AVAIL PROMO:- Creating userPromo of code ${createPromoDto.code} for user ${createPromoDto.userId}" }
        val userPromo = UserPromo()
        userPromo.userId = createPromoDto.userId
        userPromo.code = createPromoDto.code.uppercase()
        userPromo.type = createPromoDto.type
        userPromo.productType = createPromoDto.productType

        // In case applied promo is referral or affiliate we save reward configurations
        val isValidForAffiliate = createPromoDto.promo?.isAffiliate == true && createPromoDto.promo.affiliateId != null
        val isReferralPromo = createPromoDto.type == PromoType.REFERRAL

        if (isValidForAffiliate || isReferralPromo) {
            val config = variableService.getRewardConfig()
            userPromo.referralRewardConfig = config
            userPromo.promoTargetProductType =
                config.promoTargetProductType ?: PromoTargetProductType.ALL_QUALIFYING_PRODUCT
        }

        createPromoDto.promo?.customPromoData?.let { userPromo.customPromoData = it }
        createPromoDto.referrerSource?.let { userPromo.referrerSource = it }
        createPromoDto.channel?.let { userPromo.channel = it }
        // Adding details to track if this userPromo added through migrations
        createPromoDto.isCreatedWithMigration?.let {
            userPromo.isCreatedWithMigration = it
        }
        createPromoDto.promo?.promoTargetProductType?.let {
            userPromo.promoTargetProductType = it
        }

        try {
            userPromoRepository.save(userPromo).also {
                processUserPromo(userPromo)
            }
            logger.info { "promo ${createPromoDto.code} saved for user: ${createPromoDto.userId}" }
            return userPromo
        } catch (e: DataIntegrityViolationException) {
            logger.info { "promo ${createPromoDto.code} already consumed by user: ${createPromoDto.userId}" }
            throw PromoConsumedException()
        }
    }

    private fun processUserPromo(userPromo: UserPromo) {
        val isValidForFreeTrade =
            userPromo.customPromoData?.rewards?.flatMap { it.value }?.any { it.validForFreeTrade() } ?: false

        if (!isValidForFreeTrade) return
        val payload = mapOf(
            "userPromoId" to userPromo.id
        )
        taskService.register(TaskName.PROCESS_USER_PROMO, payload)
    }

    /**
     * this method apply affiliate/referral promo for the new service if the Client already applied one affiliate/
     * referral promo and which is in APPLIED State
     *
     * @param user
     */
    fun applyAffiliateOrReferralPromo(user: User, productType: ProductType): UserPromo? {
        return try {
            createUserPromoEntryIfEligible(user, productType)
        } catch (e: Exception) {
            logger.info { "No Affiliate/Referral promo is valid for user ${user.id} in $productType" }
            null
        }
    }

    /**
     * this method find the exact match of the user promo and the promo for affiliate/referral
     *
     */
    fun getPromoAndUserPromoAppliedByUser(user: User): Pair<UserPromo, Promo?> {
        val userPromos =
            userPromoRepository.findByUserIdAndStatusInAndDeleted(user.id!!, listOf(UserPromoStatus.APPLIED), false)
        if (userPromos.isEmpty() || userPromos.size > 1) {
            throw IllegalStateException("userPromo can't be null")
        }
        return when (userPromos[0].type) {
            PromoType.REFERRAL -> return Pair(userPromos[0], null)
            else -> getAffiliatePair(userPromos)
        }
    }
    fun createUserPromoEntryIfEligible(user: User, productType: ProductType): UserPromo {
        val (userPromo, promo) = getPromoAndUserPromoAppliedByUser(user)
        if (userPromo.productType == null || userPromo.productType == productType) throw IllegalStateException("userPromo is already available")
        // Create only if new account is created within Fulfillment Period
        if (userPromoService.isWaitingPeriodCompleted(userPromo, promo, user)) {
            throw IllegalStateException("userPromo cannot be created for $productType as fulfillment period is over the promo")
        }
        return createPromo(CreatePromoDto(userPromo.code, user.id!!, promo, userPromo.type, productType))
    }
    private fun getAffiliatePair(userPromos: List<UserPromo>): Pair<UserPromo, Promo> {
        val affiliatePromos = promoRepository.findAllByCodeIn(userPromos.map { it.code }).filter { it.isAffiliate && it.ruleEngine?.validForWealthAndTradePromotion() ?: false }
        val userPromo = userPromos.firstOrNull { it.code in affiliatePromos.map { promo -> promo.code } }
            ?: throw IllegalStateException("userPromo can't be null")
        val promo = affiliatePromos.firstOrNull { it.code == userPromo.code }
            ?: throw IllegalStateException("affiliate promo can't be null")
        return Pair(userPromo, promo)
    }

    /**
     * This function applies the code provided for userId and email
     *
     * @param [userId] user id of user
     * @param [code] promo code which is applied
     * @param [email] email of user
     *
     * @return [UserPromoHistoryInfo] the user promo avail info.
     */
    fun availPromoCodeV2(userId: Long, code: String, email: String? = null): UserPromoAvailInfo {
        val userPromo = availPromoCode(userId, code, email)
        return convertToUserPromoAvailInfo(userPromo)
    }

    /**
     * This function convert the userPromo to UserPromoAvailInfo
     *
     * @param [userPromo] userPromo which is needed to convert
     *
     * @return [UserPromoAvailInfo] the final converted userPromo avail info
     */
    private fun convertToUserPromoAvailInfo(userPromo: UserPromo): UserPromoAvailInfo {
        return UserPromoAvailInfo(
            id = userPromo.id!!,
            header = userPromo.code,
            type = userPromo.type,
            status = UserPromoHistoryStatus.UNDER_REVIEW.displayText,
            createdAt = userPromo.createdAt!!,
            message = localizationService.getMessage("promo.valid-promo")
        )
    }

    fun isReferralCode(code: String): Boolean {
        val referrer = userRepository.findByReferralCode(code.uppercase())
        return (referrer != null)
    }

    private fun validateReferralCode(userId: Long, code: String) {
        validateStacking(userId, isReferral = true)

        if (isUserAlreadyReferred(userId)) throw ReferralCodeAlreadyUsedException()
        val referrer = userRepository.findByReferralCode(code)!!
        if (referrer.id == userId) throw OwnReferralCodeUseException()

        val transitions = transitionRepository
            .findAllByUserIdAndStageAndStatus(userId, Stage.AML, AmlStatus.APPROVED.name)

        if (transitions.isNotEmpty()) {
            val amlDate = transitions.map { it.createdAt!! }.maxOrNull()!!
            if (amlDate < Date().addDays(-14)) {
                logger.info("Referral period expired for userId $userId as AML date $amlDate is older than 14 days")
                throw ReferralPeriodExpiredException()
            }
        }

        validateNewClientForReferralUserPromo(userId)
    }

    // if user availed the benefit of any promo, he can't use any new user promo (Referral, promo with First deposit type)
    private fun validateNewClientForReferralUserPromo(userId: Long) {
        val userPromos = userPromoRepository.findAllByUserIdAndStatusInAndDeleted(userId, UserPromoStatus.getAwardedTerminalStates(), false)

        if (userPromos.isNotEmpty()) throw PromoNotEligibleException()
    }

    /**
     * This method check if the user is new client of not for the promo, if user received the benefit for a particular code,
     * they won't be able to apply the new client promos for both the services.
     * If more than one ruleEngine is available in Promo consider as accountLevel promo and only be applied by one time
     *
     * @param userId the user applying the code
     * @param ruleEngineTypes the list of rule Engine for this promo
     */
    fun validateNewClientUserPromo(userId: Long, ruleEngineTypes: List<RuleEngineType>) {
        val userPromos = userPromoRepository.findAllByUserIdAndStatusInAndDeleted(userId, UserPromoStatus.getAwardedTerminalStates(), false)
        val (tradeUserPromos, wealthUserPromos) = userPromos.partition {
            it.validRuleEngine?.isTradeRuleEngineType() ?: false
        }

        val hasUserReceivedRewardsAndApplyingAccountLevelPromo = ruleEngineTypes.size > 1 && userPromos.isNotEmpty()

        // trade new promo is not applicable if user received any reward of trade promo
        val hasUserReceivedAnyTradeRewardAndApplyingNewTradePromo = RuleEngineType.FIRST_TRADE in ruleEngineTypes && tradeUserPromos.isNotEmpty()

        // wealth new promo is not applicable if user received any reward of wealth promo
        val hasUserReceivedAnyWealthRewardAndApplyingNewWealthPromo = RuleEngineType.FIRST_DEPOSIT in ruleEngineTypes && wealthUserPromos.isNotEmpty()

        if (hasUserReceivedRewardsAndApplyingAccountLevelPromo || hasUserReceivedAnyTradeRewardAndApplyingNewTradePromo || hasUserReceivedAnyWealthRewardAndApplyingNewWealthPromo) {
            throw PromoNotEligibleException()
        }
    }

    // checks if a user has already availed a referral code
    private fun isUserAlreadyReferred(userId: Long): Boolean {
        val userPromos = userPromoRepository.findAllByUserId(userId)

        return userPromos.any { it.type == PromoType.REFERRAL && !it.deleted }
    }

    fun getPromo(code: String): Promo {
        return promoRepository.findByCode(code.uppercase()) ?: throw PromoInvalidException()
    }

    private fun validatePromoApplication(user: User, promo: Promo, email: String?) {
        validatePromo(promo)

        checkReusability(promo, user.id!!)

        validateStacking(user.id!!, promo, isReferral = false)

        checkForUniqueUsersApplicability(user.id!!, promo, email)

        checkUserPromoAllowed(user, promo)

        promo.ruleEngine?.getTypes()?.let {
            // todo remove this code once the latest trade rule engine get added.
            if (it.any { ruleEngineType -> ruleEngineType.isNewClientRuleEngine() } && promo.code != "28CNY") {
                validateNewClientUserPromo(user.id!!, it)
            }
        }

        // todo remove this code once the latest trade rule engine get added.
        if (promo.code != "28CNY") {
            checkForOldActiveTradingUser(promo, user.id!!)
        }
    }

    /**
     * In case old user applies trade promo, we won't allow it
     * We throw exception if active user apply promo code and
     * their portfolio funded before TradePromoReleaseDate
     *
     */
    private fun checkForOldActiveTradingUser(promo: Promo, userId: Long) {
        portfolioService.getActiveTradingPortfolio(userId)?.let {
            val tradePromoReleaseDate = getTradePromoReleaseDate()
            val isActivePortfolioFunded = alfredPromoService.isActivePortfolioFundedTillDate(it, tradePromoReleaseDate.toLocalDate())
            it.createdAt?.let { createdAt ->
                if (promo.ruleEngine?.getTypes()?.size == 1 &&
                    promo.ruleEngine?.getTypes()?.contains(RuleEngineType.FIRST_TRADE) == true &&
                    createdAt < tradePromoReleaseDate && isActivePortfolioFunded
                ) throw StackingNotAllowedException()
            }
        }
    }

    /**
     * Return release date for the trade promo
     */
    fun getTradePromoReleaseDate(): Date {
        return geoPromoConstants.getTradePromoReleaseDate()
    }

    /**
     * This function validates if the promo could be applied by user and is not
     * from different ServiceType. [com.syfe.backend.common.enums.ServiceType]
     *
     * @param [user] user entity
     *
     * @param [promo] Applied promo by user
     */
    private fun checkUserPromoAllowed(user: User, promo: Promo) {
        val promoRuleEngines = promo.ruleEngine?.getTypes() ?: emptyList()

        if (promo.type == PromoType.CUSTOM && (
                    kycUtils.isKycApproved(user.id!!, ServiceType.ACTIVE_TRADING) || kycUtils.isKycApproved(user.id!!, ServiceType.WEALTH)
                    )
        ) throw InvalidUserPromoException(localizationService.getMessage("error.eligible_for_new_client"))

        if (user.isActiveTrading && user.isWealth) return
        if (user.isActiveTrading && !RuleEngineType.getTradeRules().any { it in promoRuleEngines }) throw InvalidUserPromoException(localizationService.getMessage("error.promo_only_allowed_for_wealth"))
        if (user.isWealth && !RuleEngineType.getWealthRules().any { it in promoRuleEngines }) throw InvalidUserPromoException(localizationService.getMessage("error.promo_only_allowed_for_trade"))
    }

    private fun checkForUniqueUsersApplicability(userId: Long, promo: Promo, email: String?) {
        // Checking for the case if the user is an unregistered one and is allowed to apply the promo
        // during signup as a part of marketing campaign
        // Note: email will only be not-null when this fun is called at the time of signup/adding funds
        if (email != null) {
            if (!isUnRegisteredClientAllowedToApply(email, promo)) {
                logger.info { "User $email not allowed to avail code ${promo.code}" }
                throw PromoInvalidException()
            }
            return
        }

        if (!isUserAllowedToApply(userId, promo)) {
            logger.info { "User $userId not allowed to avail code ${promo.code}" }
            throw PromoInvalidException()
        }
    }

    private fun isUserAllowedToApply(userId: Long, promo: Promo): Boolean {
        return if (promo.allowedUserIds.isNullOrEmpty()) {
            promo.allowedUnRegisteredEmails.isNullOrEmpty()
        } else promo.allowedUserIds!!.contains(userId)
    }

    private fun isUnRegisteredClientAllowedToApply(email: String, promo: Promo): Boolean {
        return if (promo.allowedUnRegisteredEmails.isNullOrEmpty()) {
            promo.allowedUserIds.isNullOrEmpty()
        } else promo.allowedUnRegisteredEmails!!.contains(email)
    }

    // Checks if already there are user promos stacked (,i.e, currently in non-terminal state)
    private fun validateStacking(userId: Long, promo: Promo? = null, isReferral: Boolean) {
        val userPromos =
            userPromoRepository.findByUserIdAndStatusInAndDeleted(userId, UserPromoStatus.getNonTerminalStates(), false)
                .filter {
                    it.code != BASE_PROMOTION_CODE
                }

        // Check custom stackable logic
        if (!isReferral && isCustomStackablePromo(promo!!.code)) {
            validateCustomStackingLogic(userPromos, promo)
            return
        }
        // If all already applied promos are custom stackable, then allow non custom stackable promo
        if (userPromos.all { isCustomStackablePromo(it.code) }) return

        // Normal stacking logic should be checked among only non custom stackable promo
        if (isStackingAllowed(userPromos.filter { !isCustomStackablePromo(it.code) }, promo, isReferral)) return

        if (userPromos.isNotEmpty()) throw StackingNotAllowedException()
    }

    // checks whether the case is an exceptional one where we allow stacking
    private fun isStackingAllowed(
        userPromos: List<UserPromo>,
        promo: Promo? = null,
        isReferral: Boolean
    ): Boolean {
        // Case 0: (Special) Client will not able to apply more than one SPECIAL_ASX_PROMO_CODE at a time.
        promo?.let {
            val isSpecialPromoAlreadyApplied = userPromos.any { it.code in SPECIAL_ASX_PROMO_CODES }
            val isCurrentPromoSpecial = promo.code in SPECIAL_ASX_PROMO_CODES
            if (isSpecialPromoAlreadyApplied && isCurrentPromoSpecial)
                return false
        }

        // Case 1: Client will be able to apply a cash-bonus/special promo on the top of referral code
        //              if the code to be applied is of latest deposit type or code is FreeTrade code
        promo?.let {
            val isOnlyAppliedPromoReferral = userPromos.size == 1 && userPromos[0].type == PromoType.REFERRAL
            val isCashBonusPromoOfLatestDepositType =
                promo.type == PromoType.PROMO && promo.ruleEngine?.getTypes()
                    ?.contains(RuleEngineType.LATEST_DEPOSIT) ?: false
            // todo remove this after ASX special period end
            val isSpecialPromoCode = promo.code in SPECIAL_ASX_PROMO_CODES
            if (isOnlyAppliedPromoReferral && (isCashBonusPromoOfLatestDepositType || isSpecialPromoCode)) {
                return true
            }
        }

        // Case 2: Client will be able to apply a referral on the top of cash bonus promo
        //              if the cash bonus promo is of latest deposit type
        if (isReferral) {
            val isOnlyAppliedPromoCashBonus = userPromos.size == 1 && userPromos[0].type == PromoType.PROMO
            if (isOnlyAppliedPromoCashBonus) {
                val isAppliedCashBonusPromoOfLatestDepositType =
                    getPromo(userPromos[0].code).ruleEngine?.ruleEngineType == RuleEngineType.LATEST_DEPOSIT
                if (isAppliedCashBonusPromoOfLatestDepositType) {
                    return true
                }
            }
        }
        // Case 3: Client will be able to apply a stackable promo over any stackable promo
        //         a promo is stackable when stackable column is true and for referral is stackable with any stackable promo
        //          two referrals can't be stackable
        if (!(userPromos.any { it.type == PromoType.REFERRAL } && isReferral)) {
            val isCurrentPromoStackable = promo?.stackable ?: true
            val promoCodes = userPromos.map { it.code }
            val areAppliedPromoStackable = promoRepository.findAllByCodeIn(promoCodes).map { it.stackable }.all { it }

            if (areAppliedPromoStackable && isCurrentPromoStackable) return true
        }

        return false
    }

    private fun checkReusability(promo: Promo, userId: Long) {
        val userPromos = userPromoRepository.findAllByUserIdAndCodeAndDeleted(
            userId,
            promo.code,
            deleted = false
        )
        val userPromoNonTerminal = userPromos.filter { it.status in UserPromoStatus.getNonTerminalStates() }
        val userPromoTerminal = userPromos.filter { it.status in UserPromoStatus.getTerminalStates() }
        if ((userPromoNonTerminal.isNotEmpty()) || (userPromoTerminal.isNotEmpty() && !promo.reusable)) {
            throw PromoConsumedException()
        }
    }

    private fun validatePromo(promo: Promo) {
        val now = Date()
        if (now < promo.validFrom) throw PromoInvalidException()
        if (now > promo.validTill) throw PromoExpiredException()
        if (promo.isSuspended()) throw PromoSuspendedException()

        promo.usageLimit?.let {
            val numberOfUserPromos = userPromoRepository.countByCode(promo.code)
            if (numberOfUserPromos >= it) throw PromoCapExceededException()
        }

        if (promo.code == BASE_PROMOTION_CODE) throw PromoInvalidException()
    }

    /**
     * On Sign Up
     * Returns the message for [code] and [email] (optional) stating whether code is applicable for user ?
     */
    fun validatePromoCodeAtSignup(code: String, email: String? = null): Map<String, String> {
        if (!isReferralCode(code)) {
            val promo = getPromo(code)
            validatePromo(promo)

            if (promo.userSettingsDetails?.eligibleClientType !in EligibleClientType.getSignUpEligibleClientTypes()) throw PromoNotEligibleException()

            email?.let {
                if (!isUnRegisteredClientAllowedToApply(email, promo)) {
                    logger.info { "User $email not allowed to avail code ${promo.code}" }
                    throw PromoInvalidException()
                }
            }
        }
        return mapOf("message" to localizationService.getMessage("promo.valid-promo"))
    }

    /**
     * On Have a Promo/Invitation code ? pop up
     * Returns the message for [user], [code] and [email] (mandatory) stating whether code is applicable for user ?
     */
    fun validatePromoCode(user: User, code: String, email: String): Map<String, String> {
        if (isReferralCode(code)) {
            validateReferralCode(user.id!!, code.uppercase())
            assert(isProductTypeValid(ProductType.WEALTH, user, PromoType.REFERRAL) || isProductTypeValid(ProductType.TRADE, user, PromoType.REFERRAL))
        } else {
            val promo = getPromo(code)
            validatePromoApplication(user, promo, email)
            assert(isProductTypeValid(ProductType.WEALTH, user, promo.type, promo) || isProductTypeValid(ProductType.TRADE, user, promo.type, promo))
        }
        return mapOf("message" to localizationService.getMessage("promo.verify-promo"))
    }

    fun createPromo(request: CreatePromoRequest) {
        val promoCode = request.code!!.uppercase()
        val existingPromo = promoRepository.findByCode(promoCode)
        if (existingPromo != null) {
            throw PromoCodeAlreadyExistsException()
        }

        if (request.validTill!! < request.validFrom!!) {
            throw Exception("validTill should be newer than validFrom")
        }

        val isAffiliate = request.isAffiliate ?: false
        if (!isAffiliate && request.affiliateId != null) {
            throw InvalidUpdateUserPromoRequestException(localizationService.getMessage("error.invalid_update_user_promo_request.invalid_affiliate_id"))
        }

        logger.info { "Creating promo code: $promoCode" }
        promoRepository.save(
            Promo().apply {
                code = promoCode
                reusable = request.reusable!!
                validFrom = request.validFrom
                validTill = request.validTill
                type = PromoType.valueOf(request.type!!)
                campaignType = (request.campaignType)?.let { CampaignType.valueOf(it) }
                this.isAffiliate = isAffiliate
                affiliateId = request.affiliateId
                stackable = request.stackable
                request.customPromoData?.let {
                    customPromoData = it.convertToCustomPromoData()
                }
                request.userSettingsDetailsDto?.let {
                    userSettingsDetails = it.convertToUserSettingsDetails()
                }
                request.ruleEngineDto?.let {
                    ruleEngine = it.convertToRuleEngine()
                }
                request.allowedUsers?.let {
                    allowedUserIds = userService.getUsersByEmails(it).map { user -> user.id!! }
                }
                request.allowedUnRegisteredClients?.let {
                    allowedUnRegisteredEmails = it
                }
                request.promoUsageLimit?.let {
                    usageLimit = it
                }
                request.affiliateName?.let {
                    affiliateName = it
                }
                request.promoTargetProductType?.let {
                    promoTargetProductType = it
                }
            }
        )
    }

    fun updatePromo(code: String, request: UpdatePromoRequest) {
        val promo = promoRepository.findByCode(code.uppercase()) ?: throw PromoInvalidException()
        val columnAuditRecords = mutableListOf<ColumnEditRecord>()

        fun addColumnAuditRecords(columnName: String, oldValue: String, newValue: String) {
            columnAuditRecords.add(
                ColumnEditRecord(
                    columnName = columnName,
                    oldValue = oldValue,
                    newValue = newValue,
                    delta = null
                )
            )
        }

        request.type?.let {
            addColumnAuditRecords("type", promo.type.toString(), it)
            promo.type = PromoType.valueOf(it)
        }

        request.campaignType?.let {
            addColumnAuditRecords("campaignType", promo.campaignType.toString(), it)
            promo.campaignType = CampaignType.valueOf(it)
        }

        request.reusable?.let {
            addColumnAuditRecords("reusable", promo.reusable.toString(), it.toString())
            promo.reusable = it
        }

        request.validFrom?.let {
            addColumnAuditRecords("validFrom", promo.validFrom.toString(), it.toString())
            promo.validFrom = it
        }

        request.validTill?.let {
            addColumnAuditRecords("validTill", promo.validTill.toString(), it.toString())
            promo.validTill = it
        }

        request.stackable?.let {
            addColumnAuditRecords("stackable", promo.stackable.toString(), it.toString())
            promo.stackable = it
        }

        request.promoTargetProductType?.let {
            addColumnAuditRecords("promoTargetProductType", promo.promoTargetProductType.toString(), it.toString())
            promo.promoTargetProductType = it
        }

        val isAffiliate = request.isAffiliate ?: false
        if (!isAffiliate && request.affiliateId != null) {
            throw InvalidUpdateUserPromoRequestException(localizationService.getMessage("error.invalid_update_user_promo_request.invalid_affiliate_id"))
        }

        request.isAffiliate?.let {
            addColumnAuditRecords("isAffiliate", promo.isAffiliate.toString(), it.toString())
            promo.isAffiliate = request.isAffiliate
            if (!promo.isAffiliate) {
                addColumnAuditRecords("affiliateId", promo.affiliateId.toString(), null.toString())
                promo.affiliateId = null
            }
        }

        request.affiliateId?.let {
            addColumnAuditRecords("affiliateId", promo.affiliateId.toString(), it.toString())
            promo.affiliateId = it
        }

        request.status?.let {
            addColumnAuditRecords("status", promo.status.toString(), it)
            promo.status = PromoStatus.valueOf(it)
        }

        request.customPromoData?.let {
            addColumnAuditRecords("customPromoData", objectMapper.writeValueAsString(promo.customPromoData), objectMapper.writeValueAsString(it))
            promo.customPromoData = it.convertToCustomPromoData()
        }

        request.userSettingsDetailsDto?.let {
            addColumnAuditRecords("userPromoSettingsDetails", objectMapper.writeValueAsString(promo.userSettingsDetails), objectMapper.writeValueAsString(it))
            promo.userSettingsDetails = it.convertToUserSettingsDetails()
        }

        request.ruleEngineDto?.let {
            addColumnAuditRecords("ruleEngine", objectMapper.writeValueAsString(promo.ruleEngine), objectMapper.writeValueAsString(it))
            promo.ruleEngine = it.convertToRuleEngine()
        }

        request.allowedUsers?.let {
            addColumnAuditRecords("allowedUserIds", promo.allowedUserIds.toString(), it.toString())
            promo.allowedUserIds = userService.getUsersByEmails(it).map { user -> user.id!! }
        }

        request.allowedUnRegisteredClients?.let {
            addColumnAuditRecords(
                "allowedUnRegisteredEmails",
                promo.allowedUnRegisteredEmails.toString(),
                it.toString()
            )
            promo.allowedUnRegisteredEmails = it
        }

        request.promoUsageLimit?.let {
            addColumnAuditRecords("usageLimit", promo.usageLimit.toString(), it.toString())
            promo.usageLimit = it
        }

        request.affiliateName?.let {
            addColumnAuditRecords("affiliateName", promo.affiliateName.toString(), it)
            promo.affiliateName = it
        }

        promoRepository.save(promo)

        auditService.recordAction(
            promo.id.toString(),
            "promos",
            columnAuditRecords,
            "retool",
            AuditAction.UPDATE
        )
        logger.info { "Updated promo code: $code" }
    }

    @Transactional(rollbackFor = [Exception::class])
    fun updateUserPromo(id: Long, request: UpdateUserPromoRequest, updatedBy: String) {
        val userPromo = userPromoRepository.findByIdOrNull(id)
            ?: throw InvalidUpdateUserPromoRequestException(
                localizationService.getMessage("error.invalid_update_user_promo_request.promo_not_found")
            )

        validateRequest(request, userPromo)

        val columnAuditRecords = mutableListOf<ColumnEditRecord>()

        fun addColumnAuditRecords(columnName: String, oldValue: String, newValue: String) {
            columnAuditRecords.add(
                ColumnEditRecord(
                    columnName = columnName,
                    oldValue = oldValue,
                    newValue = newValue,
                    delta = null
                )
            )
        }

        request.deleted?.let {
            if (it && isPromoFeeWaiver(userPromo.code)) {
                fmsInvestorService.resetFeeTierForUser(userPromo)
            }
            addColumnAuditRecords("deleted", userPromo.deleted.toString(), it.toString())
            userPromo.deleted = it
        }

        request.transitionMessages?.let {
            val newStatus = request.status?.let { it1 -> UserPromoStatus.valueOf(it1) }
            if (newStatus != null) {
                userPromo.addUserPromoStatusManualUpdateDetails(newStatus, request.transitionMessages!![0])
            }
        }

        request.status?.let {
            val newStatus = UserPromoStatus.valueOf(request.status)
            addColumnAuditRecords("status", userPromo.status.toString(), newStatus.toString())
            userPromo.addUserPromoStatusTransition(newStatus, request.transitionMessages)
        }

        request.productType?.let {
            addColumnAuditRecords("productType", userPromo.productType.toString(), it)
            userPromo.productType = ProductType.valueOf(it)
        }

        request.clientBonusAmount?.let {
            addColumnAuditRecords("clientBonusAmount", userPromo.clientBonusAmount.toString(), it.toString())
            userPromo.clientBonusAmount = request.clientBonusAmount
        }

        request.customPromoData?.let {
            val patch = it.convertToCustomPromoData()
            val customPromoData = userPromo.customPromoData

            val feeTierId = patch.feeTierId ?: customPromoData?.feeTierId
            val validity = patch.validity ?: customPromoData?.validity
            val capAmount = patch.capAmount ?: customPromoData?.capAmount
            val cashBonusAmount = patch.cashBonusAmount ?: customPromoData?.cashBonusAmount
            val lockInAmount = patch.lockInAmount ?: customPromoData?.lockInAmount
            val lockInPeriodInDays = patch.lockInPeriodInDays ?: customPromoData?.lockInPeriodInDays
            val bonusAmount = patch.bonusAmounts ?: customPromoData?.bonusAmounts
            val rewards = patch.rewards ?: customPromoData?.rewards

            userPromo.customPromoData = CustomPromoData(
                feeTierId,
                validity,
                capAmount,
                cashBonusAmount,
                lockInAmount,
                lockInPeriodInDays,
                bonusAmount,
                rewards
            )
            addColumnAuditRecords("customPromoData", objectMapper.writeValueAsString(customPromoData), objectMapper.writeValueAsString(userPromo.customPromoData))
        }

        request.transferInAmount?.let {
            addColumnAuditRecords("transferInAmount", userPromo.transferInAmount.toString(), it.toString())
            userPromo.transferInAmount = request.transferInAmount

            // updating tier based on transferIn Amount
            val tier = request.tier ?: userPromo.getBonusTier(request.transferInAmount)
            addColumnAuditRecords("tier", userPromo.tier.toString(), tier.toString())
            userPromo.tier = tier
        }

        request.investmentTransferInAmount?.let {
            addColumnAuditRecords(
                "investmentTransferInAmount",
                userPromo.investmentTransferInAmount.toString(),
                it.toString()
            )
            userPromo.investmentTransferInAmount = request.investmentTransferInAmount
        }

        request.incomeTransferInAmount?.let {
            addColumnAuditRecords("incomeTransferInAmount", userPromo.incomeTransferInAmount.toString(), it.toString())
            userPromo.incomeTransferInAmount = request.incomeTransferInAmount
        }

        request.validRuleEngine?.let {
            addColumnAuditRecords("validRuleEngine", userPromo.validRuleEngine.toString(), it)
            userPromo.validRuleEngine = RuleEngineType.valueOf(it)
        }

        request.cashFlowId?.let {
            addColumnAuditRecords("cashFlowId", userPromo.cashFlowId.toString(), it.toString())
            userPromo.cashFlowId = it
        }

        request.portfolioId?.let {
            val portfolioType = request.portfolioType?.let { type -> PortfolioType.valueOf(type) }
                ?: portfolioService.findPortfolioByIdAndUserId(it, userPromo.userId!!)!!.type

            addColumnAuditRecords("portfolioId", userPromo.portfolioId.toString(), it.toString())
            addColumnAuditRecords("portfolioType", userPromo.portfolioType.toString(), portfolioType.toString())
            userPromo.portfolioId = it
            userPromo.portfolioType = portfolioType
        }

        request.clawbackPortfolioId?.let {
            addColumnAuditRecords("clawbackPortfolioId", userPromo.clawbackPortfolioId.toString(), it.toString())
            userPromo.clawbackPortfolioId = it
        }

        request.referrerPortfolioId?.let {
            addColumnAuditRecords("referrerPortfolioId", userPromo.referrerPortfolioId.toString(), it.toString())
            userPromo.referrerPortfolioId = it
        }

        request.referralRewardConfig?.let {
            addColumnAuditRecords("referralRewardConfig", userPromo.referralRewardConfig.toString(), it.toString())
            userPromo.referralRewardConfig = it
        }

        request.associatedPortfolioTypes?.let {
            addColumnAuditRecords(
                "associatedPortfolioTypes",
                userPromo.associatedPortfolioTypes.toString(),
                it.toString()
            )
            userPromo.associatedPortfolioTypes = it.map { portfolio -> PortfolioType.valueOf(portfolio) }
        }

        request.affiliateAmount?.let {
            addColumnAuditRecords("affiliateAmount", userPromo.affiliateAmount.toString(), it.toString())
            userPromo.affiliateAmount = it
        }

        request.referralBonusAmount?.let {
            addColumnAuditRecords("referralBonusAmount", userPromo.referralBonusAmount.toString(), it.toString())
            userPromo.referralBonusAmount = it
        }

        if (request.isUpdateWithMigration == true) {
            userPromo.isUpdateWithMigration = request.isUpdateWithMigration
        }

        request.promoTargetProductType?.let {
            addColumnAuditRecords("promoTargetProductType", userPromo.promoTargetProductType.toString(), it.toString())
            userPromo.promoTargetProductType = it
        }

        request.cashPlusGuaranteedLaunchBonusPortfolioId?.let {
            addColumnAuditRecords("cashPlusGuaranteedLaunchBonusPortfolioId", userPromo.cashPlusGuaranteedLaunchBonusPortfolioId.toString(), it.toString())
            userPromo.cashPlusGuaranteedLaunchBonusPortfolioId = it
        }

        auditService.recordAction(
            userPromo.id.toString(),
            "userPromos",
            columnAuditRecords,
            updatedBy,
            AuditAction.UPDATE
        )

        logger.info { "Updated user promo code for user promo id: $id" }
    }

    private fun validateRequest(request: UpdateUserPromoRequest, userPromo: UserPromo) {
        if (request.portfolioId != null) {
            val portfolio = portfolioService.findPortfolioByIdAndUserId(request.portfolioId!!, userPromo.userId!!) ?: throw InvalidUpdateUserPromoRequestException(
                localizationService.getMessage("error.invalid_portfolio_id")
            )

            if (portfolio.hidden) {
                throw InvalidUpdateUserPromoRequestException(
                    localizationService.getMessage("error.hidden_portfolio")
                )
            }

            if (userPromo.productType == ProductType.WEALTH && portfolio.type !in geoPortfolioService.getWealthPortfolioTypes()) {
                throw InvalidUpdateUserPromoRequestException(
                    localizationService.getMessage("error.invalid_portfolio_type_wealth")
                )
            }

            if (userPromo.productType == ProductType.TRADE && portfolio.type != PortfolioType.ACTIVE_TRADING) {
                throw InvalidUpdateUserPromoRequestException(
                    localizationService.getMessage("error.invalid_portfolio_type_trade")
                )
            }
        }

        if (userPromo.type == PromoType.REFERRAL) {

            // validate in case of referral promo referrer portfolio id should be present
            if (request.referrerPortfolioId == null) {
                throw InvalidUpdateUserPromoRequestException(
                    localizationService.getMessage("error.referral_promo_referrer_portfolio_id_required")
                )
            }

            // validate referrer portfolio id is present in portfolios list  in case of referral promo
            // 1. get the code from user promo table
            // 2. get the referrer id from users table using that code
            // 3. use that id in portfolios table to check whether portfolio exists for user or not.
            val referrerId = userService.getUserByReferralCode(userPromo.code!!).id!!
            portfolioService.findPortfolioByIdAndUserId(request.referrerPortfolioId!!, referrerId)
                ?: throw InvalidUpdateUserPromoRequestException(
                    localizationService.getMessage("error.invalid_referrer_portfolio_id")
                )
        }

        // validate affiliate id is present in portfolios list in case of affiliate promo if affiliate id and referrer portfolio id  is present
        val promo = promoRepository.findByCode(userPromo.code!!)
        if (promo?.isAffiliate == true && promo.affiliateId != null && request.referrerPortfolioId != null) {
            portfolioService.findPortfolioByIdAndUserId(request.referrerPortfolioId!!, promo.affiliateId!!)
                ?: throw InvalidUpdateUserPromoRequestException(
                    localizationService.getMessage("error.invalid_affiliate_portfolio_id")
                )
        }
    }

    private fun isPromoFeeWaiver(code: String): Boolean {
        val promo = promoRepository.findByCode(code)

        return promo?.type == PromoType.FEE_WAIVER
    }

    fun deleteUserPromo(id: Long) {
        val userPromo = userPromoRepository.findByIdOrNull(id)
            ?: throw InvalidUserPromoException(
                localizationService.getMessage("error.invalid_user_promo")
            )

        if (userPromo.status in UserPromoStatus.getClawbackNonTerminalStates()) {
            promoClawbackService.triggerClawback(userPromo)
        }

        userPromo.deleted = true
        userPromoRepository.save(userPromo)

        if (isPromoFeeWaiver(userPromo.code)) {
            fmsInvestorService.resetFeeTierForUser(userPromo)
        }
    }

    // hard delete for promo
    fun deletePromo(code: String) {
        val promo = promoRepository.findByCode(code.uppercase()) ?: throw PromoInvalidException()
        val userPromos = userPromoRepository.findAllByCodeAndStatusIn(code, UserPromoStatus.getNonTerminalStates())
        if (userPromos.isEmpty()) {
            promoRepository.delete(promo)
        } else throw PromoInvalidDeleteException()
    }

    fun getNewFlashCoffeePromo(userId: Long, customPromoType: CustomPromoType): List<CustomPromo> {
        val sort = Sort.by("id")
        val pageableRequest: Pageable = PageRequest.of(0, 1, sort)
        val newFlashCoffeePromos = customPromoRepository.findAllByStatusAndType(CustomPromoStatus.NEW, customPromoType, pageableRequest)
        if (newFlashCoffeePromos.isEmpty()) {
            // send Slack message for exhausted promos case
            val errorMessage = "Custom Promo of type *${customPromoType.name}* not available for user with Id: *$userId*"
            logger.critical(errorMessage, AlertType.SLACK)
            throw CustomPromoNotFoundException()
        }
        return newFlashCoffeePromos
    }

    fun saveCustomPromos(userId: Long, customPromos: List<CustomPromo>) {
        try {
            customPromoRepository.saveAll(customPromos)
        } catch (e: Exception) {
            val errorMessage = "Custom Promo notification sent to User but status of Promo not changed in db for user with Id: *$userId* and Promo type *${customPromos[0].type}* and Promo Code: *${customPromos[0].code}*"
            logger.critical(errorMessage, AlertType.SLACK)
        }
    }

    private fun getReferralAndDisplayNameAndType(code: String): ReferralCodeData {
        return if (isReferralCode(code)) {
            getDisplayNameAndTypeForReferralCode(code)
        } else {
            getDisplayNameAndTypeForAffiliateCode(code)
        }
    }

    private fun getDisplayNameAndTypeForReferralCode(code: String): ReferralCodeData {
        val productType = geoPromoConstants.getReferralProductType()
        val user = userRepository.findByReferralCode(code.uppercase()) ?: throw PromoInvalidException()
        return getReferralCodeData(user.id!!, user.firstName!!, type = "Referrer", productType)
    }

    private fun getDisplayNameAndTypeForAffiliateCode(code: String): ReferralCodeData {
        promoRepository.findByCode(code.uppercase())?.let { promo ->
            val productType = userPromoService.getProductType(promo)
            if (promo.isAffiliate) {
                val user = promo.affiliateId?.let { userService.getUserById(it) }
                val firstName = promo.affiliateName ?: user?.let { (it.firstName!! + " " + it.lastName!!) }
                val userId = user?.let { user.id!! }
                return firstName?.let { getReferralCodeData(userId, it, "Affiliate", productType) } ?: throw PromoInvalidException()
            } else throw PromoInvalidException()
        } ?: throw PromoInvalidException()
    }

    private fun getReferralCodeData(userId: Long?, firstName: String, type: String, productType: ProductType): ReferralCodeData {
        return ReferralCodeData(
            userId = userId,
            firstName = firstName,
            type = type,
            productType = productType
        )
    }

    fun getReferralPromoDetails(code: String): ReferralCodeDetails {
        val referralCodeData = getReferralAndDisplayNameAndType(code)
        return ReferralCodeDetails(
            firstName = referralCodeData.firstName,
            referrerId = referralCodeData.userId,
            referrerAppliedCode = referralCodeData.userId?.let { getReferrerCode(it) },
            tradeBonus = geoPromoConstants.getRefereeBonusAmountInTrade(),
            maxFeeWaiverMonths = geoPromoConstants.getMaxFeeWaiverMonths(),
            noOfTrades = geoPromoConstants.getMaxNumberOfTrades(),
            reward = geoPromoConstants.getMaxWealthAndTradeBonus(),
            currencySymbol = localizationService.getMessage("referral.promo.currency"),
            promoType = referralCodeData.type,
            productType = referralCodeData.productType
        )
    }

    /**
     * @return Sends applied referrer or affiliate code for provided user id
     */
    private fun getReferrerCode(userId: Long): String? {
        val userPromos = userPromoRepository.findAllByUserId(userId)
        userPromos.firstOrNull { it.type == PromoType.REFERRAL }?.code?.let { return it }

        return promoRepository.findAllByCodeIn(userPromos.filter { it.type == PromoType.PROMO }.map { it.code })
            .firstOrNull { it.isAffiliate && it.affiliateId != null }?.code
    }

    /**
     * Creates referral details based on provided service type
     * @return [ReferralDetails] based on [ServiceType]
     */
    fun getReferralDetails(user: User, services: List<ServiceType>): Map<ServiceType, ReferralDetails> {
        val skippedPromoRewards = userPromoRewardsRepository.findAllByUserIdAndStatus(user.id!!, UserPromoRewardStatus.SKIP)

        val referrals = userPromoService.getPromoByCodeAndStatus(user.referralCode, UserPromoStatus.getReferralCompletedStates())
            .filter { it.id !in skippedPromoRewards.map { promoReward -> promoReward.userPromoId } }

        return services.associateWith { serviceType ->
            val referralsInService = referrals.filter {
                (it.validRuleEngine ?: RuleEngineType.FIRST_DEPOSIT) in getValidRule(serviceType)
            }
            val (message, link) = getGeoReferralDetails(user, serviceType)

            ReferralDetails(
                currency = geoContext.getDefaultCurrency(),
                referralAmount = when (serviceType) {
                    ServiceType.ACTIVE_TRADING -> geoPromoConstants.getMaxTradeBonus()
                    ServiceType.WEALTH -> geoPromoConstants.getMaxRewardAmount()
                    ServiceType.COMMON, ServiceType.CASH -> throw IllegalArgumentException("Service type $serviceType not supported")
                },
                referralCount = referralsInService.size,
                referralEarnings = referralsInService.sumOf { it.referralBonusAmount ?: BigDecimal(0) },
                message = message,
                link = link
            )
        }
    }

    private fun getGeoReferralDetails(user: User, serviceType: ServiceType): Pair<String, String> {
        val generalInfo = localizationService.getMessage("referral.content.general_info")
        val referralInviteUrlPath = localizationService.getMessage("referral.link.invite.url.path")

        return when (geoContext.getGeography()) {
            Geography.SG -> Pair(
                generalInfo + " \n" + WEBSITE_BASE_URL + referralInviteUrlPath + "${serviceType.value}/" + user.referralCode,
                referralInviteUrlPath + "${serviceType.value}/" + user.referralCode
            )
            Geography.AU -> Pair(
                generalInfo + " \n" + WEBSITE_BASE_URL + referralInviteUrlPath + user.referralCode,
                referralInviteUrlPath + user.referralCode
            )
            Geography.HK -> Pair(
                generalInfo + " \n" + WEBSITE_BASE_URL + referralInviteUrlPath + user.referralCode,
                referralInviteUrlPath + user.referralCode
            )
        }
    }

    private fun getValidRule(serviceType: ServiceType): List<RuleEngineType> {
        return when (serviceType) {
            ServiceType.WEALTH -> RuleEngineType.getWealthRules()
            ServiceType.ACTIVE_TRADING -> RuleEngineType.getTradeRules()
            ServiceType.COMMON, ServiceType.CASH -> throw IllegalArgumentException("Service type $serviceType not supported")
        }
    }

    /**
     * This method create duplicate promo
     *
     * @param request the request body contain promo code, count and length which need to duplicate
     */
    fun createDuplicatePromos(request: DuplicatePromoRequest) {
        val promo = promoRepository.findByCode(request.code!!)
            ?: throw IllegalArgumentException("Code is not a valid promo")

        var newCodes = mutableSetOf<String>()

        // If there are pre-existing codes, use them
        if (!request.codes.isNullOrEmpty()) {
            newCodes = request.codes.toMutableSet()
        } else {
            while (newCodes.size < request.count!!) {
                val newCode = getRandomString(request.length!!)
                newCodes.add(promo.code + newCode)
            }
        }

        val promos = newCodes.map {
            Promo().apply {
                code = it
                reusable = promo.reusable
                validFrom = promo.validFrom
                validTill = promo.validTill
                type = promo.type
                isAffiliate = promo.isAffiliate
                affiliateId = promo.affiliateId
                stackable = promo.stackable
                customPromoData = promo.customPromoData
                meta = promo.meta
                campaignType = promo.campaignType
                status = promo.status
            }
        }
        promoRepository.saveAll(promos)
    }

    /**
     * This method create Alpha Numeric code to append in code
     */
    private fun getRandomString(length: Int): String {
        val allowedChars = ('A'..'Z') + ('0'..'9')
        return (1..length)
            .map { allowedChars.random() }
            .joinToString("")
    }

    fun getPromosByCodes(codes: List<String>): List<Promo> {
        return promoRepository.findAllByCodeIn(codes).toList()
    }

    fun migratePromos() {
        val fetchNextPromosBatch = { page: Int ->
            val sort = Sort.by("createdAt")
            val pageableRequest: Pageable = PageRequest.of(page, 5000, sort)
            promoRepository.findAllByIsAffiliate(true, pageableRequest)
        }
        logger.info { "Started migration of Promo" }
        var page = 0
        var promos = fetchNextPromosBatch(page)
        while (promos.isNotEmpty()) {
            promos.forEach { promo ->
                try {
                    migratePromo(promo)
                    logger.info { "Migration for promo ${promo.id} and code ${promo.code}" }
                } catch (e: Exception) {
                    logger.error(e) { "Error occurred while migrating promo ${promo.id} and promo code is ${promo.code}." }
                }
            }
            promoRepository.saveAll(promos)
            logger.info { "Completed migration of ${promos.size} Promo" }
            promos = fetchNextPromosBatch(++page)
        }
    }

    private fun migratePromo(promo: Promo) {
        promo.userSettingsDetails = UserSettingsDetails(
            EligibleClientType.NET_NEW_CLIENT,
            geoPromoConstants.getPromoGracePeriod(),
            FulfillmentRule.X_DAYS_FROM_CODE_APPLICATION,
            geoPromoConstants.getPromoFulfillmentPeriod()
        )
    }

    fun bulkUpdatePromos(newPromosList: List<PromoCodeMetaUpdateRequest>): Int {
        var updatedPromoCount = 0
        newPromosList.forEach { promo ->
            try {
                val updatePromoRequest = objectMapper.convert<UpdatePromoRequest>(promo.promoCodeMetaDto)!!
                updatePromoRequest.userSettingsDetailsDto?.validate()
                updatePromo(promo.code, updatePromoRequest)
                updatedPromoCount += 1
                logger.info {
                    "Updated the promo ${promo.code} with campaignType as ${updatePromoRequest.campaignType} " +
                            "and user settings details -: eligible client type as ${updatePromoRequest.userSettingsDetailsDto?.eligibleClientType}, " +
                            "grace period equal to ${updatePromoRequest.userSettingsDetailsDto?.gracePeriod ?: 0} days, with fulfillment rule as ${updatePromoRequest.userSettingsDetailsDto?.fulfillmentRule} " +
                            "and fulfillment period equal to ${updatePromoRequest.userSettingsDetailsDto?.fulfillmentPeriod} days."
                }
            } catch (e: Exception) {
                logger.info {
                    "Unable to update the promo ${promo.code} with values equal to ${promo.promoCodeMetaDto}"
                }
            }
        }
        return updatedPromoCount
    }

    /**
     * This Method is use to avail bulk promo for list of user which face issue with signup
     * @param availPromoRequest object of [AvailPromoRequest]
     */
    fun availPromos(availPromoRequest: AvailPromoRequest) {
        try {
            val userPromo = userPromoRepository.findAllByUserIdAndCode(availPromoRequest.userId, availPromoRequest.code)
            val promo = promoRepository.findByCode(availPromoRequest.code)
            if (userPromo.isEmpty() && promo.isNullOrBlank())
                availPromoCode(availPromoRequest.userId, availPromoRequest.code)
            else throw IllegalStateException("Either UserPromo is already Applied for user or Promo is invalid")
        } catch (e: Exception) {
            logger.error(e) { "BulkAvailPromo: Error while availing promo for user ${availPromoRequest.userId} and code ${availPromoRequest.code}" }
        }
    }
}
