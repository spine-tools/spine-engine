######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

module SpineREPL

using Sockets
using REPL
using REPL.REPLCompletions
using REPL.Terminals
using REPL.LineEdit

send_msg(host, port) = send_msg(host, port, "")
send_msg(host, port, msg::Nothing) = send_msg(host, port, "")
function send_msg(host, port, msg)
    s = connect(host, port)
    write(s, msg)
    close(s)
end

function completions(text)
    join(completion_text.(REPLCompletions.completions(text, length(text))[1]), " ")
end

# Create MIState to work with history. This works with julia 1.0 to 1.6
term = TerminalBuffer(IOBuffer())
repl = LineEditREPL(term, false)
repl.history_file = true
interface = REPL.setup_interface(repl)
mistate = LineEdit.init_state(term, interface)

history_item(index) = LineEdit.mode(mistate).hist.history[end + 1 - index]

function add_history(line)
    prompt_state = LineEdit.state(mistate)
    hist = LineEdit.mode(mistate).hist
    take!(prompt_state.input_buffer)
    write(prompt_state.input_buffer, line)
    REPL.add_history(hist, prompt_state)
end

end


