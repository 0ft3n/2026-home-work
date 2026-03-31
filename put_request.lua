counter = 0

request = function()
    counter = counter + 1
    local key = "wrk_key_" .. counter
    local value = string.rep("x", math.random(1, 256))
    local path = "/v0/entity?id=" .. key

    return wrk.format("PUT", path, {}, value)
end

