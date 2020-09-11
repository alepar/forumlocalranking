package io.alepar.flocal.download

import com.googlecode.lanterna.TextColor
import com.googlecode.lanterna.gui2.*
import com.googlecode.lanterna.screen.TerminalScreen
import com.googlecode.lanterna.terminal.DefaultTerminalFactory
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import kotlin.system.exitProcess

const val barWidth = 35

class LanternaUI(private val progress: GlobalProgress) {

    private val gui: MultiWindowTextGUI
    private val timeLabel: Label

    private val progressBarPanel: Panel
    private val userListProgressBar: ProgressBar

    private val userRatingsProgressBar: ProgressBar
    private val userProfilesProgressBar: ProgressBar
    private val userBoardPostsProgressBar: ProgressBar
    private val userBoardRatingsProgressBar: ProgressBar

    private val startNanos = System.nanoTime()

    private var userListDone = false

    init {
        val terminal = DefaultTerminalFactory().createTerminal()
        val screen = TerminalScreen(terminal)
        screen.startScreen()

        gui = MultiWindowTextGUI(screen, DefaultWindowManager(), EmptySpace(TextColor.ANSI.BLUE))

        userListProgressBar = ProgressBar(0, userListUrls().size).apply { preferredWidth = barWidth }
        timeLabel = Label("00:00:00")
        progressBarPanel = Panel().apply {
            addComponent(timeLabel)
            addComponent(userListProgressBar)
        }

        userRatingsProgressBar = ProgressBar(0, 0).apply { preferredWidth = barWidth }
        userProfilesProgressBar = ProgressBar(0, 0).apply { preferredWidth = barWidth }
        userBoardPostsProgressBar = ProgressBar(0, 0).apply { preferredWidth = barWidth }
        userBoardRatingsProgressBar = ProgressBar(0, 0).apply { preferredWidth = barWidth }

        gui.addWindow(BasicWindow("Progress").apply { component = progressBarPanel })
        gui.guiThread.processEventsAndUpdate()
    }

    fun watch() {
        while(!Thread.interrupted()) {
            updateUI()
            gui.guiThread.processEventsAndUpdate()

            if (progress.isDone()) {
                Thread.sleep(1000L)
                progress.finalize()
                gui.screen.stopScreen()
                exitProcess(0)
            }

            Thread.sleep(1L)
        }
    }

    private fun updateUI() {
        val curNanos = System.nanoTime() - startNanos
        timeLabel.text = duration(curNanos)

        if (!userListDone && progress.userListProgress.done && progress.userRatingsProgress.max>0) {
            userListDone = true
            progressBarPanel.apply {
                removeComponent(userListProgressBar)

                addComponent(userProfilesProgressBar)
                addComponent(userRatingsProgressBar)
                addComponent(userBoardPostsProgressBar)
                addComponent(userBoardRatingsProgressBar)
            }
        }

        if (userListDone) {
            progress.userProfilesProgress.update(userProfilesProgressBar)
            progress.userRatingsProgress.update(userRatingsProgressBar)
            progress.userBoardPostsProgress.update(userBoardPostsProgressBar)
            progress.userBoardRatingsProgress.update(userBoardRatingsProgressBar)
        } else {
            progress.userListProgress.update(userListProgressBar)
        }
    }

}

fun duration(nanos: Long): String =
        LocalTime.ofNanoOfDay(nanos).format(DateTimeFormatter.ofPattern("HH:mm:ss"))
