package io.alepar.flocal.download

import com.googlecode.lanterna.gui2.ProgressBar
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger

class GlobalProgress(private val finalizer: () -> Unit) {

    private val log = LoggerFactory.getLogger(GlobalProgress::class.java)

    val userListProgress = FlowProgress("User List")

    val userRatingsProgress = FlowProgress("User Ratings")
    val userProfilesProgress = FlowProgress("User Profiles")
    val userBoardPostsProgress = FlowProgress("User Board Posts")
    val userBoardRatingsProgress = FlowProgress("User Board Ratings")

    fun finalize() {
        try {
            finalizer.invoke()
        } catch (e: Exception) {
            log.error("finalizer threw", e)
        }
    }

    fun isDone(): Boolean {
        return userListProgress.done
                && userRatingsProgress.done
                && userProfilesProgress.done
                && userBoardPostsProgress.done
                && userBoardRatingsProgress.done
    }

}

class FlowProgress(val name: String) {

    @Volatile var cur = AtomicInteger()
    @Volatile var max: Int = 0
    @Volatile var done: Boolean = false

    fun increment() {
        cur.incrementAndGet()
    }

    fun update(bar: ProgressBar) {
        bar.max = max
        bar.value = cur.get()
        bar.labelFormat = "$name: $cur/$max"
    }
}

