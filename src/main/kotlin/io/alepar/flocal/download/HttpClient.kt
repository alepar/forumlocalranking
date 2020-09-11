package io.alepar.flocal.download

import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl
import java.util.concurrent.Semaphore

val httpClient: AsyncHttpClient = Dsl.asyncHttpClient(Dsl.config()
        .setMaxRequestRetry(3)
)
val httpSemaphore = Semaphore(concurrency)
fun download(url: String): String {
    httpSemaphore.acquire()
    try {
        return httpClient.prepareGet(url).execute().get().responseBody
    } finally {
        httpSemaphore.release()
    }
}