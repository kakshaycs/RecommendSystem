import android.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

class AuthService {
    private val secretKey = "18261KAJSH"
    private val initVector = "DIRECTOR1234567"

    fun login(username: String, password: String): Boolean {
        println("username: $username password: $password")
        val encryptedUsername = encryptString(username)
        val encryptedPassword = encryptString(password)

        // Here, you would typically send the encrypted username and password to a server for authentication
        // and return the result of the authentication process

        // For the sake of this example, we'll just check if the credentials match
        return encryptedUsername == "encrypted_username" && encryptedPassword == "encrypted_password"
    }

    private fun encryptString(input: String): String {
        val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
        val key = SecretKeySpec(secretKey.toByteArray(), "AES")
        val iv = IvParameterSpec(initVector.toByteArray())

        cipher.init(Cipher.ENCRYPT_MODE, key, iv)
        val encrypted = cipher.doFinal(input.toByteArray())
        return Base64.encodeToString(encrypted, Base64.DEFAULT)
    }

    private fun decryptString(encrypted: String): String {
        val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
        val key = SecretKeySpec(secretKey.toByteArray(), "AES")
        val iv = IvParameterSpec(initVector.toByteArray())

        cipher.init(Cipher.DECRYPT_MODE, key, iv)
        val decrypted = cipher.doFinal(Base64.decode(encrypted, Base64.DEFAULT))
        return String(decrypted)
    }
}