package com.syfe.backend.promo

import com.syfe.backend.promo.constants.PROMO_CODE_SKIP_LIST
import com.syfe.backend.promo.exceptions.StackingNotAllowedException
import com.syfe.backend.promo.models.Promo
import com.syfe.backend.promo.models.UserPromo

/**
 * This method return true is the promo code is for Income portfolio or false
 * @param code promo code
 */
fun isIncomePromoCode(code: String): Boolean {
    return (!code.startsWith("SRP") && (code.endsWith("RB") || code.endsWith("INCOME")))
}

/**
 * This method return true if the promo code is for SRS portfolio or false
 * @param code promo code
 */
fun isSrsPromoCode(code: String): Boolean {
    return code.startsWith("SRS")
}

/**
 * This method return true if the promo code is for Protected portfolio or false
 * @param code promo code
 */
fun isProtectedPromoCode(code: String): Boolean {
    return code.startsWith("DSP")
}

fun isCustomStackablePromo(code: String) =
    isIncomePromoCode(code) || isSrsPromoCode(code) || isProtectedPromoCode(code)

/**
 * Validates whether a given promo can be stacked with the user's existing promos.
 *
 * Stacking Rules:
 * - Income promos cannot be stacked with other income promos but can be stacked with all other promo types.
 * - SRS promos cannot be stacked with other SRS promos but can be stacked with all other promo types.
 * - Protected promos cannot be stacked with other Protected promos but can be stacked with all other promo types.
 *
 * @param userPromos List of promos already applied to the user.
 * @param promo The new promo to validate for stacking. If null, no validation is performed.
 *
 * @throws StackingNotAllowedException if the stacking rules are violated.
 */
fun validateCustomStackingLogic(userPromos: List<UserPromo>, promo: Promo) {
    if (!isCustomStackablePromo(promo.code)) return
    if (userPromos.any { existingPromo ->
            isProtectedPromoCode(promo.code) && isProtectedPromoCode(existingPromo.code) ||
                    isIncomePromoCode(promo.code) && isIncomePromoCode(existingPromo.code) ||
                    isSrsPromoCode(promo.code) && isSrsPromoCode(existingPromo.code)
        }
    ) throw StackingNotAllowedException()
}

/**
 * This method return true if we need to skip processing for the promo code in Promo State Machine
 * We won't process the promo if the promo is :-
 * * Promo code in the [PROMO_CODE_SKIP_LIST]
 * * isIncome promo
 *
 * @param code promo code
 */
fun promoCodeToSkipProcessing(code: String): Boolean {
    val promoCodeInSkipList = code in PROMO_CODE_SKIP_LIST

    return promoCodeInSkipList || isIncomePromoCode(code) || isProtectedPromoCode(code)
}
